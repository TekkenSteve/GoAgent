package state

// Layer identifies the persistence layer for a field or group in the state model.
// Used by the hot/warm/cold state layering pattern in Temporal workflow definitions.
type Layer string

const (
	// LayerHot identifies the hot persistence layer.
	LayerHot Layer = "hot"
	// LayerWarm identifies the warm persistence layer.
	LayerWarm Layer = "warm"
	// LayerCold identifies the cold persistence layer.
	LayerCold Layer = "cold"
)
