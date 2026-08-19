package core

import (
	"reflect"
	"testing"
)

func TestCardWireRoundTripPreservesInteractiveElements(t *testing.T) {
	original := &Card{
		Header: &CardHeader{Title: "Reasoning", Color: "orange"},
		Elements: []CardElement{
			CardMarkdown{Content: "current: auto"},
			CardDivider{},
			CardActions{
				Buttons: []CardButton{{
					Text: "Apply", Type: "primary", Value: "act:/reasoning 2",
					Extra: map[string]string{"source": "card"},
				}},
				Layout: CardActionLayoutEqualColumns,
			},
			CardNote{Text: "usage", Tag: "reasoning"},
			CardListItem{
				Text: "low", BtnText: "Choose", BtnType: "default",
				BtnValue: "act:/reasoning 2", Extra: map[string]string{"level": "low"},
			},
			CardSelect{
				Placeholder: "Select", InitValue: "act:/reasoning 1",
				Options: []CardSelectOption{
					{Text: "auto", Value: "act:/reasoning 1"},
					{Text: "low", Value: "act:/reasoning 2"},
				},
			},
		},
	}

	roundTripped, err := cardFromWire(cardToWire(original))
	if err != nil {
		t.Fatalf("cardFromWire() error = %v", err)
	}
	if !reflect.DeepEqual(roundTripped, original) {
		t.Fatalf("round-tripped card mismatch:\n got: %#v\nwant: %#v", roundTripped, original)
	}
}
