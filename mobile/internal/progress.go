package internal

// ProgressListener 上传进度回调
// 由 gomobile 生成对应的 Kotlin 抽象类
type ProgressListener interface {
	OnUploadStart(filename string, totalBytes int)
	OnUploadProgress(filename string, bytesReceived int, totalBytes int)
	OnUploadComplete(filename string, bytesReceived int, success bool)
	OnUploadError(filename string, message string)
}

// ProgressDispatcher 空实现兜底
type ProgressDispatcher struct {
	Listener ProgressListener
}

func (d *ProgressDispatcher) Start(name string, total int) {
	if d.Listener != nil {
		d.Listener.OnUploadStart(name, total)
	}
}

func (d *ProgressDispatcher) Progress(name string, cur, total int) {
	if d.Listener != nil {
		d.Listener.OnUploadProgress(name, cur, total)
	}
}

func (d *ProgressDispatcher) Complete(name string, cur int, ok bool) {
	if d.Listener != nil {
		d.Listener.OnUploadComplete(name, cur, ok)
	}
}

func (d *ProgressDispatcher) Error(name, msg string) {
	if d.Listener != nil {
		d.Listener.OnUploadError(name, msg)
	}
}
