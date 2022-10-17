package podlabel

func DefaultAppSelectorAdmin() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlogadm",
		"app.kubernetes.io/component": "admin",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorMetaRepos() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlogmr",
		"app.kubernetes.io/component": "metadata-repository",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorMetaReposClear() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlogmr-clear",
		"app.kubernetes.io/component": "metadata-repository",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorStorageNode() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlogsn",
		"app.kubernetes.io/component": "storage-node",
		"app.kubernetes.io/part-of":   "varlog",
	}
}

func DefaultAppSelectorStorageNodeClear() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "varlogsn-clear",
		"app.kubernetes.io/component": "storage-node",
		"app.kubernetes.io/part-of":   "varlog",
	}
}
