package retry

// The ladder of nine-docs/adr/0001, and the only place Go describes it.
//
// @MustafaKemalV asked for this on the review of CO4.3, and the reason is the
// history: 0001 exists because three places described the chain and no two
// agreed, and a `tiers` integer passed in at the call site would have been the
// fourth. Here the number of rounds is the length of the list, so changing the
// ladder is one edit and nothing else can disagree with it.
//
// topics.sh in nine-platform creates these, and it is the other description.
// The two are checked against each other in TestTheChainMatchesTheTopicScript,
// which reads the script rather than trusting this comment.
var Chain = []string{
	"events",          // the source
	"events.retry-5m", // round 1
	"events.retry-1h", // round 2
}

// Parked is where a message goes when it has used every tier: a good payload
// behind a dependency that was down, waiting for a person to replay the lot.
// DLQ is where a Poison goes on first sight, because it never processes and
// the useful action is a person reading it. Both by 0001, decision 3.
const (
	Parked = "events.parked"
	DLQ    = "events.dlq"
)

// Tiers is how many retry rounds the chain allows, which is every topic in it
// but the source.
func Tiers() int { return len(Chain) - 1 }

// NextTopic is where a record fails forward to. The second return is false
// when there is nowhere left, which is the record's last round and sends it to
// Parked rather than round the chain again.
func NextTopic(from string) (string, bool) {
	for i, t := range Chain {
		if t == from && i+1 < len(Chain) {
			return Chain[i+1], true
		}
	}
	return "", false
}

// InChain reports whether a topic is part of the ladder. A record read from
// somewhere else, events.parked during a replay for instance, is not on the
// ladder and must not be forwarded along it.
func InChain(topic string) bool {
	for _, t := range Chain {
		if t == topic {
			return true
		}
	}
	return false
}
