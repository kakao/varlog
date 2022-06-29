package podlabel

func DefaultAppSelectorAdmin() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlog",
		"app.kubernetes.io/component": "varlogadm",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorMetaRepos() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlog",
		"app.kubernetes.io/component": "varlogmr",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorMetaReposClear() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlog",
		"app.kubernetes.io/component": "varlogmr-clear",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorStorageNode() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlog",
		"app.kubernetes.io/component": "varlogsn",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorStorageNodeClear() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlog",
		"app.kubernetes.io/component": "varlogsn-clear",
		"app.kubernetes.io/part-of":   "varlog",
	}
}
