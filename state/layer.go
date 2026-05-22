package state

// Layer identifies the persistence layer for a field or group in the state model.
// Used by the hot/warm/cold state layering pattern in Temporal workflow definitions.
type Layer string

const (
	LayerHot  Layer = "hot"
	LayerWarm Layer = "warm"
	LayerCold Layer = "cold"
)
