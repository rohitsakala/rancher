package notifications

type EventCategory struct {
	Category     string
	EventReasons []string
}

var knownCategories = map[string]EventCategory{
	"cert-expiry": {
		Category: "cert-expiry",
		EventReasons: []string{
			"CertificateExpirationWarning",
			"CACertificateExpirationWarning",
		},
	},
}

var reasonToCategory = func() map[string]EventCategory {
	m := make(map[string]EventCategory)
	for _, cat := range knownCategories {
		for _, reason := range cat.EventReasons {
			m[reason] = cat
		}
	}
	return m
}()
