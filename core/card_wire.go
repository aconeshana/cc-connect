package core

import "fmt"

// CardWire is the tagged internal representation used when a Feishu card
// callback is received by a cc-connect process other than the TUI owner.
// CardElement is an interface and cannot be decoded safely by encoding/json
// without an explicit discriminator.
type CardWire struct {
	Header   *CardHeader       `json:"header,omitempty"`
	Elements []CardElementWire `json:"elements,omitempty"`
}

type CardElementWire struct {
	Type     string           `json:"type"`
	Markdown string           `json:"markdown,omitempty"`
	Buttons  []CardButton     `json:"buttons,omitempty"`
	Layout   CardActionLayout `json:"layout,omitempty"`
	NoteText string           `json:"note_text,omitempty"`
	NoteTag  string           `json:"note_tag,omitempty"`
	ListItem *CardListItem    `json:"list_item,omitempty"`
	Select   *CardSelect      `json:"select,omitempty"`
}

func cardToWire(card *Card) *CardWire {
	if card == nil {
		return nil
	}
	wire := &CardWire{Header: card.Header, Elements: make([]CardElementWire, 0, len(card.Elements))}
	for _, element := range card.Elements {
		switch value := element.(type) {
		case CardMarkdown:
			wire.Elements = append(wire.Elements, CardElementWire{Type: "markdown", Markdown: value.Content})
		case CardDivider:
			wire.Elements = append(wire.Elements, CardElementWire{Type: "divider"})
		case CardActions:
			wire.Elements = append(wire.Elements, CardElementWire{
				Type: "actions", Buttons: value.Buttons, Layout: value.Layout,
			})
		case CardNote:
			wire.Elements = append(wire.Elements, CardElementWire{
				Type: "note", NoteText: value.Text, NoteTag: value.Tag,
			})
		case CardListItem:
			copy := value
			wire.Elements = append(wire.Elements, CardElementWire{Type: "list_item", ListItem: &copy})
		case CardSelect:
			copy := value
			wire.Elements = append(wire.Elements, CardElementWire{Type: "select", Select: &copy})
		default:
			return nil
		}
	}
	return wire
}

func cardFromWire(wire *CardWire) (*Card, error) {
	if wire == nil {
		return nil, nil
	}
	card := &Card{Header: wire.Header, Elements: make([]CardElement, 0, len(wire.Elements))}
	for _, element := range wire.Elements {
		switch element.Type {
		case "markdown":
			card.Elements = append(card.Elements, CardMarkdown{Content: element.Markdown})
		case "divider":
			card.Elements = append(card.Elements, CardDivider{})
		case "actions":
			card.Elements = append(card.Elements, CardActions{Buttons: element.Buttons, Layout: element.Layout})
		case "note":
			card.Elements = append(card.Elements, CardNote{Text: element.NoteText, Tag: element.NoteTag})
		case "list_item":
			if element.ListItem == nil {
				return nil, fmt.Errorf("routed card list_item is missing payload")
			}
			card.Elements = append(card.Elements, *element.ListItem)
		case "select":
			if element.Select == nil {
				return nil, fmt.Errorf("routed card select is missing payload")
			}
			card.Elements = append(card.Elements, *element.Select)
		default:
			return nil, fmt.Errorf("unsupported routed card element %q", element.Type)
		}
	}
	return card, nil
}
