package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

type hostResumeOutcome struct {
	Session              AgentSessionInfo
	AlreadyActive        bool
	ActivationGeneration uint64
}

func (e *Engine) hostResumeLock(sessionKey string) *sync.Mutex {
	e.hostResumeMu.Lock()
	defer e.hostResumeMu.Unlock()
	lock := e.hostResumeLocks[sessionKey]
	if lock == nil {
		lock = &sync.Mutex{}
		e.hostResumeLocks[sessionKey] = lock
	}
	return lock
}

func newHostActivationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate activation ID: %w", err)
	}
	return "cc-" + hex.EncodeToString(raw[:]), nil
}

func validateHostResumeSessionID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 1024 {
		return "", fmt.Errorf("invalid host session ID")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("invalid host session ID")
		}
	}
	return value, nil
}

func (e *Engine) resumeHostSession(
	sessionKey string, requested AgentSessionInfo,
) (hostResumeOutcome, error) {
	lock := e.hostResumeLock(sessionKey)
	lock.Lock()
	defer lock.Unlock()

	agent, sessions := e.sessionContextForKey(sessionKey)
	resumer, ok := agent.(HostSessionResumer)
	if !ok {
		return hostResumeOutcome{}, fmt.Errorf("%s", e.i18n.T(MsgResumeNotSupported))
	}
	targetID, err := validateHostResumeSessionID(requested.ID)
	if err != nil {
		return hostResumeOutcome{}, err
	}
	available, err := agent.ListSessions(e.ctx)
	if err != nil {
		return hostResumeOutcome{}, fmt.Errorf(e.i18n.T(MsgListError), err)
	}
	var target *AgentSessionInfo
	for i := range available {
		if available[i].ID == targetID {
			copy := available[i]
			target = &copy
			break
		}
	}
	if target == nil {
		return hostResumeOutcome{}, fmt.Errorf(e.i18n.T(MsgResumeNoMatch), targetID)
	}

	oldLocal := sessions.GetOrCreateActive(sessionKey)
	if oldLocal.GetAgentSessionID() == target.ID {
		state := e.interactiveState(e.interactiveKeyForSessionKey(sessionKey))
		var generation uint64
		if state != nil {
			state.mu.Lock()
			generation = state.sessionHostActivationGeneration
			state.mu.Unlock()
		}
		return hostResumeOutcome{
			Session: *target, AlreadyActive: true, ActivationGeneration: generation,
		}, nil
	}
	if !oldLocal.TryLock() {
		return hostResumeOutcome{}, fmt.Errorf("%s", e.i18n.T(MsgResumeBusy))
	}
	oldLocked := true
	defer func() {
		if oldLocked {
			oldLocal.UnlockWithoutUpdate()
		}
	}()

	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	state, err := e.ensureHostResumeState(sessionKey, interactiveKey)
	if err != nil {
		return hostResumeOutcome{}, err
	}
	activationID, err := newHostActivationID()
	if err != nil {
		return hostResumeOutcome{}, err
	}
	activation, err := resumer.ResumeHostSession(e.ctx, target.ID, activationID)
	if err != nil {
		return hostResumeOutcome{}, err
	}
	prepared := activation.Session
	discardPrepared := true
	defer func() {
		if discardPrepared && prepared != nil {
			_ = prepared.Close()
		}
	}()
	if prepared == nil || !prepared.Alive() || activation.SessionID != target.ID ||
		activation.ActivationID != activationID || activation.ActivationGeneration == 0 {
		return hostResumeOutcome{}, fmt.Errorf("invalid host activation response")
	}

	state.mu.Lock()
	currentGeneration := state.sessionHostActivationGeneration
	currentRemote := state.agentSession
	workspaceDir := state.workspaceDir
	state.mu.Unlock()
	if activation.ActivationGeneration < currentGeneration {
		return hostResumeOutcome{}, fmt.Errorf("host activation was superseded")
	}
	if activation.ActivationGeneration == currentGeneration && currentGeneration != 0 &&
		currentRemote != nil && currentRemote.CurrentSessionID() != target.ID {
		return hostResumeOutcome{}, fmt.Errorf("host activation generation conflicts with current session")
	}

	// The old reader must stop before its AgentSession pointer is replaced.
	// The Java host is already on the target session, so any target events are
	// buffered by the newly prepared Session Link attachment meanwhile.
	e.stopUnsolicitedReader(state)
	restartOldReader := true
	defer func() {
		if restartOldReader && currentRemote != nil && currentRemote.Alive() {
			e.startUnsolicitedReader(
				state, oldLocal, sessions, interactiveKey, workspaceDir)
		}
	}()

	e.interactiveMu.Lock()
	currentState := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if currentState != state {
		return hostResumeOutcome{}, fmt.Errorf("host thread binding changed during resume")
	}
	state.mu.Lock()
	if state.sessionHostActivationGeneration > activation.ActivationGeneration {
		state.mu.Unlock()
		return hostResumeOutcome{}, fmt.Errorf("host activation was superseded")
	}
	state.mu.Unlock()

	targetLocal, err := sessions.TrySwitchToAgentSession(
		sessionKey, target.ID, agent.Name(), target.Summary)
	if err != nil {
		return hostResumeOutcome{}, err
	}
	targetLocked := true
	defer func() {
		if targetLocked {
			targetLocal.UnlockWithoutUpdate()
		}
	}()
	if channel, explicit := oldLocal.GetCollaborationChannel(); explicit {
		targetLocal.SetCollaborationChannel(channel)
	}

	state.mu.Lock()
	oldRemote := state.agentSession
	state.agentSession = prepared
	state.eventsNeedResync = false
	state.sessionHostActivationGeneration = activation.ActivationGeneration
	state.stopped = false
	state.mu.Unlock()
	sessions.Save()

	restartOldReader = false
	discardPrepared = false
	oldLocked = false
	oldLocal.UnlockWithoutUpdate()

	// Hand the target Session lock to a queue drainer without leaving a window
	// where an inbound message can append after the queue check and remain
	// stranded. queueMessageForBusySession must acquire state.mu before it can
	// append; when there is no existing queue we release the Session while still
	// holding state.mu, so a newly unblocked appender will observe an unlocked
	// Session on its mandatory retry and start its own drainer.
	startReader := false
	state.mu.Lock()
	if len(state.pendingMessages) > 0 && e.startMessageWorker(func() {
		e.drainOrphanedQueue(targetLocal, sessions, interactiveKey, agent, workspaceDir)
	}) {
		targetLocked = false
	} else {
		targetLocal.UnlockWithoutUpdate()
		targetLocked = false
		startReader = true
	}
	state.mu.Unlock()
	if startReader {
		e.startUnsolicitedReader(state, targetLocal, sessions, interactiveKey, workspaceDir)
	}
	if oldRemote != nil && oldRemote != prepared {
		if err := oldRemote.Close(); err != nil {
			slog.Warn("detach previous Session Host attachment after resume",
				"session_key", sessionKey, "error", err)
		}
	}

	return hostResumeOutcome{
		Session: *target, ActivationGeneration: activation.ActivationGeneration,
	}, nil
}

func (e *Engine) ensureHostResumeState(
	sessionKey, interactiveKey string,
) (*interactiveState, error) {
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state != nil {
		return state, nil
	}
	platform := e.platformForSessionKey(sessionKey)
	if platform == nil {
		return nil, fmt.Errorf("platform not found for host thread")
	}
	reconstructor, ok := platform.(ReplyContextReconstructor)
	if !ok {
		return nil, fmt.Errorf("platform cannot reconstruct host thread")
	}
	replyCtx, err := reconstructor.ReconstructReplyCtx(sessionKey)
	if err != nil {
		return nil, fmt.Errorf("reconstruct host thread: %w", err)
	}
	created := &interactiveState{
		platform: platform, replyCtx: replyCtx, eventsNeedResync: false,
	}
	e.interactiveMu.Lock()
	if existing := e.interactiveStates[interactiveKey]; existing != nil {
		state = existing
	} else {
		e.interactiveStates[interactiveKey] = created
		state = created
	}
	e.interactiveMu.Unlock()
	return state, nil
}

func (e *Engine) platformForSessionKey(sessionKey string) Platform {
	name := extractPlatformName(sessionKey)
	for _, platform := range e.platforms {
		if platform.Name() == name {
			return platform
		}
	}
	return nil
}

func (e *Engine) renderHostResumeCard(
	sessionKey string, page int, status, color string,
) (*Card, error) {
	agent, sessions := e.sessionContextForKey(sessionKey)
	if _, ok := agent.(HostSessionResumer); !ok {
		return nil, fmt.Errorf("%s", e.i18n.T(MsgResumeNotSupported))
	}
	agentSessions, err := agent.ListSessions(e.ctx)
	if err != nil {
		return nil, fmt.Errorf(e.i18n.T(MsgListError), err)
	}
	if page < 1 {
		page = 1
	}
	if len(agentSessions) == 0 {
		return e.simpleCard(
			e.i18n.Tf(MsgResumeCardTitle, 0), "turquoise", e.i18n.T(MsgListEmpty)), nil
	}
	total := len(agentSessions)
	totalPages := (total + listPageSize - 1) / listPageSize
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * listPageSize
	end := start + listPageSize
	if end > total {
		end = total
	}
	if color == "" {
		color = "turquoise"
	}
	title := e.i18n.Tf(MsgResumeCardTitle, total)
	if totalPages > 1 {
		title = e.i18n.Tf(MsgResumeCardTitlePaged, total, page, totalPages)
	}
	cb := NewCard().Title(title, color)
	if strings.TrimSpace(status) != "" {
		cb.Markdown(status).Divider()
	}
	activeID := sessions.GetOrCreateActive(sessionKey).GetAgentSessionID()
	for i := start; i < end; i++ {
		session := agentSessions[i]
		marker := "◻"
		buttonType := "default"
		if session.ID == activeID {
			marker = "▶"
			buttonType = "primary"
		}
		displayName := sessions.GetSessionName(session.ID)
		if displayName != "" {
			displayName = "📌 " + displayName
		} else {
			displayName = strings.Join(strings.Fields(session.Summary), " ")
			if displayName == "" {
				displayName = e.i18n.T(MsgListEmptySummary)
			}
			if len([]rune(displayName)) > 40 {
				displayName = string([]rune(displayName)[:40]) + "…"
			}
		}
		cb.ListItemBtn(
			e.i18n.Tf(MsgListItem, marker, i+1, displayName, session.MessageCount,
				session.ModifiedAt.Format("01-02 15:04")),
			fmt.Sprintf("#%d", i+1), buttonType, "act:/resume "+session.ID)
	}
	var nav []CardButton
	if page > 1 {
		nav = append(nav, e.cardPrevButton(fmt.Sprintf("nav:/resume %d", page-1)))
	}
	nav = append(nav, e.cardBackButton())
	if page < totalPages {
		nav = append(nav, e.cardNextButton(fmt.Sprintf("nav:/resume %d", page+1)))
	}
	cb.Buttons(nav...)
	if totalPages > 1 {
		cb.Note(e.i18n.Tf(MsgResumePageHint, page, totalPages))
	}
	return cb.Build(), nil
}

func (e *Engine) renderHostResumeCardSafe(sessionKey string, page int) *Card {
	card, err := e.renderHostResumeCard(sessionKey, page, "", "")
	if err != nil {
		return e.simpleCard(e.i18n.T(MsgResumeCardTitleShort), "red", err.Error())
	}
	return card
}

func (e *Engine) sendHostResumeCard(
	p Platform, replyCtx any, sessionKey string, page int,
) {
	card, err := e.renderHostResumeCard(sessionKey, page, "", "")
	if err != nil {
		e.reply(p, replyCtx, err.Error())
		return
	}
	if sender, ok := p.(TrackableCardSender); ok {
		if err := e.waitOutgoing(p); err != nil {
			return
		}
		handle, err := sender.SendCardWithHandle(
			e.ctx, replyCtx, e.renderCardForPlatform(p, card))
		if err != nil {
			slog.Error("send tracked resume card", "platform", p.Name(), "error", err)
			return
		}
		e.resumeCardsMu.Lock()
		e.resumeCards[sessionKey] = &interactionCardPresentation{
			sender: sender, handle: handle,
		}
		e.resumeCardsMu.Unlock()
		return
	}
	e.replyWithCard(p, replyCtx, card)
}

func (e *Engine) cmdResume(p Platform, msg *Message, args []string) {
	agent, sessions, _, err := e.commandContext(p, msg)
	if err != nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgWsResolutionError, err))
		return
	}
	if _, ok := agent.(HostSessionResumer); !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgResumeNotSupported))
		return
	}
	if len(args) == 0 {
		e.sendHostResumeCard(p, msg.ReplyCtx, msg.SessionKey, 1)
		return
	}
	query := strings.TrimSpace(strings.Join(args, " "))
	available, err := agent.ListSessions(e.ctx)
	if err != nil {
		e.reply(p, msg.ReplyCtx, fmt.Errorf(e.i18n.T(MsgListError), err).Error())
		return
	}
	target := e.matchSession(available, sessions, query)
	if target == nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgResumeNoMatch, query))
		return
	}
	outcome, err := e.resumeHostSession(msg.SessionKey, *target)
	feedbackLock := e.hostResumeLock(msg.SessionKey)
	feedbackLock.Lock()
	err = e.fenceHostResumeFeedback(msg.SessionKey, outcome, err)
	status, color := e.hostResumeStatus(outcome, err)
	card, renderErr := e.renderHostResumeCard(msg.SessionKey, 1, status, color)
	if renderErr != nil {
		e.reply(p, msg.ReplyCtx, status)
		feedbackLock.Unlock()
		return
	}
	e.replyWithCard(p, msg.ReplyCtx, card)
	feedbackLock.Unlock()
}

func (e *Engine) hostResumeStatus(outcome hostResumeOutcome, err error) (string, string) {
	if err != nil {
		return e.i18n.Tf(MsgResumeFailed, err), "red"
	}
	if outcome.AlreadyActive {
		return e.i18n.Tf(MsgResumeAlreadyActive, shortSessionID(outcome.Session.ID)), "green"
	}
	return e.i18n.Tf(
		MsgResumeSuccess, shortSessionID(outcome.Session.ID), outcome.Session.MessageCount), "green"
}

func (e *Engine) handleHostResumeCardAction(sessionKey, sessionID string) *Card {
	sessionID, err := validateHostResumeSessionID(sessionID)
	if err != nil {
		return e.simpleCard(
			e.i18n.T(MsgResumeCardTitleShort), "red", e.i18n.Tf(MsgResumeFailed, err))
	}
	agent, _ := e.sessionContextForKey(sessionKey)
	if _, ok := agent.(HostSessionResumer); !ok {
		return e.simpleCard(
			e.i18n.T(MsgResumeCardTitleShort), "red", e.i18n.T(MsgResumeNotSupported))
	}
	available, err := agent.ListSessions(e.ctx)
	if err != nil {
		return e.simpleCard(
			e.i18n.T(MsgResumeCardTitleShort), "red", fmt.Errorf(e.i18n.T(MsgListError), err).Error())
	}
	var target *AgentSessionInfo
	for i := range available {
		if available[i].ID == sessionID {
			copy := available[i]
			target = &copy
			break
		}
	}
	if target == nil {
		return e.simpleCard(
			e.i18n.T(MsgResumeCardTitleShort), "red", e.i18n.Tf(MsgResumeNoMatch, sessionID))
	}
	loading := e.simpleCard(
		e.i18n.T(MsgResumeCardTitleShort), "orange", e.i18n.T(MsgResumeLoading))
	e.pushHostResumeCardAndWait(sessionKey, loading)
	outcome, err := e.resumeHostSession(sessionKey, *target)
	feedbackLock := e.hostResumeLock(sessionKey)
	feedbackLock.Lock()
	defer feedbackLock.Unlock()
	err = e.fenceHostResumeFeedback(sessionKey, outcome, err)
	status, color := e.hostResumeStatus(outcome, err)
	card, renderErr := e.renderHostResumeCard(sessionKey, 1, status, color)
	if renderErr != nil {
		card = e.simpleCard(e.i18n.T(MsgResumeCardTitleShort), "red", renderErr.Error())
	}
	if e.pushHostResumeCardAndWait(sessionKey, card) {
		card.Elements = append(card.Elements, CardNote{Tag: ResumeCardOwnerUpdatedTag})
	}
	return card
}

// fenceHostResumeFeedback is called under the per-thread resume lock. A
// higher-generation local activation may complete after the remote Session
// Link request but before its card callback renders. In that case the old
// callback must report superseded instead of repainting the newer winner as a
// successful switch to the stale target.
func (e *Engine) fenceHostResumeFeedback(
	sessionKey string, outcome hostResumeOutcome, resumeErr error,
) error {
	if resumeErr != nil || strings.TrimSpace(outcome.Session.ID) == "" {
		return resumeErr
	}
	_, sessions := e.sessionContextForKey(sessionKey)
	if sessions.GetOrCreateActive(sessionKey).GetAgentSessionID() != outcome.Session.ID {
		return fmt.Errorf("host activation was superseded")
	}
	state := e.interactiveState(e.interactiveKeyForSessionKey(sessionKey))
	if state == nil || outcome.ActivationGeneration == 0 {
		return nil
	}
	state.mu.Lock()
	currentGeneration := state.sessionHostActivationGeneration
	state.mu.Unlock()
	if currentGeneration != outcome.ActivationGeneration {
		return fmt.Errorf("host activation was superseded")
	}
	return nil
}

func (e *Engine) pushHostResumeCard(sessionKey string, card *Card) {
	e.pushHostResumeCardWithMode(sessionKey, card, false)
}

func (e *Engine) pushHostResumeCardAndWait(sessionKey string, card *Card) bool {
	return e.pushHostResumeCardWithMode(sessionKey, card, true)
}

func (e *Engine) pushHostResumeCardWithMode(sessionKey string, card *Card, wait bool) bool {
	updated := false
	platform := e.platformForSessionKey(sessionKey)
	if platform != nil {
		if refresher, ok := platform.(CardRefresher); ok {
			if err := refresher.RefreshCard(e.ctx, sessionKey, card); err != nil {
				slog.Warn("refresh clicked resume card", "session_key", sessionKey, "error", err)
			} else {
				updated = true
			}
		}
	}
	e.resumeCardsMu.Lock()
	presentation := e.resumeCards[sessionKey]
	e.resumeCardsMu.Unlock()
	if presentation != nil {
		if wait {
			updated = e.updateInteractionCardAndWait(presentation, card) || updated
		} else {
			e.updateInteractionCard(presentation, card)
		}
	}
	return updated
}

func (e *Engine) refreshLatestResumeCard(sessionKey string) {
	e.resumeCardsMu.Lock()
	presentation := e.resumeCards[sessionKey]
	e.resumeCardsMu.Unlock()
	if presentation == nil {
		return
	}
	card, err := e.renderHostResumeCard(sessionKey, 1, "", "")
	if err != nil {
		slog.Warn("render latest resume card", "session_key", sessionKey, "error", err)
		return
	}
	if !e.updateInteractionCardAndWait(presentation, card) {
		slog.Warn("refresh latest resume card was not confirmed", "session_key", sessionKey)
	}
}

func parseResumePage(args string) int {
	page, err := strconv.Atoi(strings.TrimSpace(args))
	if err != nil || page < 1 {
		return 1
	}
	return page
}
