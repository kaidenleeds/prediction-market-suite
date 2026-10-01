package server

// sampleEvidenceBucket keeps the human-facing sample colors descriptive rather than
// promotional. n is the number of distinct settled venue+ticker contracts; the verdict
// engine's confidence sequence remains the authority for proof.
type sampleEvidenceBucket struct {
	Key   string
	Emoji string
	Label string
}

func evidenceSampleBucket(n int) sampleEvidenceBucket {
	switch {
	case n <= 10:
		return sampleEvidenceBucket{Key: "red", Emoji: "🔴", Label: "minimal sample"}
	case n <= 40:
		return sampleEvidenceBucket{Key: "orange", Emoji: "🟠", Label: "small sample"}
	case n <= 120:
		return sampleEvidenceBucket{Key: "yellow", Emoji: "🟡", Label: "growing sample"}
	case n <= 500:
		return sampleEvidenceBucket{Key: "green", Emoji: "🟢", Label: "useful sample"}
	case n <= 1000:
		return sampleEvidenceBucket{Key: "blue", Emoji: "🔵", Label: "large sample"}
	default:
		return sampleEvidenceBucket{Key: "purple", Emoji: "🟣", Label: "very large sample"}
	}
}
