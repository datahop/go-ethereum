package topicindex

// TableStats describes retained cache and admission-control structures. Expired
// entries remain counted until Expire runs. Access must be serialized with writes.
type TableStats struct {
	Capacity             int `json:"capacity"`
	Occupancy            int `json:"occupancy"`
	OccupiedTopics       int `json:"occupied_topics"`
	IPv4Nodes            int `json:"ipv4_tree_nodes"`
	IPv6Nodes            int `json:"ipv6_tree_nodes"`
	IPv4Entries          int `json:"ipv4_entries"`
	IPv6Entries          int `json:"ipv6_entries"`
	TopicBounds          int `json:"topic_bounds"`
	EffectiveTopicBounds int `json:"effective_topic_bounds"`
	EffectiveIPBounds    int `json:"effective_ip_bounds"`
}

// Occupancy returns the number of stored advertisements, including pending expiry.
func (tab *TopicTable) Occupancy() int { return tab.all.Len() }

// TopicSize returns the number of stored advertisements for a topic.
func (tab *TopicTable) TopicSize(topic TopicID) int { return tab.topicSize(topic) }

// Stats traverses the admission trees without changing them or expiring entries.
func (tab *TopicTable) Stats() TableStats {
	now := tab.config.Clock.Now()
	s := TableStats{Capacity: tab.config.AdCacheSize, Occupancy: tab.all.Len(), OccupiedTopics: len(tab.reg), TopicBounds: len(tab.wt.topicBounds), IPv4Entries: tab.wt.ipv4.count(), IPv6Entries: tab.wt.ipv6.count()}
	var walk func(*ipTreeNode) int
	walk = func(n *ipTreeNode) int {
		if n == nil {
			return 0
		}
		if n.bound.remaining(now) > 0 {
			s.EffectiveIPBounds++
		}
		return 1 + walk(n.left) + walk(n.right)
	}
	s.IPv4Nodes = walk(tab.wt.ipv4.root)
	s.IPv6Nodes = walk(tab.wt.ipv6.root)
	for _, b := range tab.wt.topicBounds {
		if b.remaining(now) > 0 {
			s.EffectiveTopicBounds++
		}
	}
	return s
}
