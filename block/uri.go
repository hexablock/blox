package block

import (
	"net/url"
)

const (
	SchemeMemory = "memory"
	SchemeFile   = "file"
	SchemeTCP    = "tcp"
)

//type DescOpener func(u *URI) (io.ReadWriteCloser, error)

type URI struct {
	*url.URL
	// Open ReadWriteCloser
	//fh io.ReadWriteCloser
	// Handler to open a file
	//FileOpen DescOpener
	// Handler to open a network connection
	//TCPOpen DescOpener
}

func NewURI(str string) *URI {

	uri := &URI{
	// FileOpen: func(u *URI) (io.ReadWriteCloser, error) {
	// 	if u.ReadOnly() {
	// 		return os.Open(u.Path)
	// 	}
	// 	return os.Create(u.Path)
	// },
	// TCPOpen: func(u *URI) (io.ReadWriteCloser, error) {
	// 	return nil, fmt.Errorf("tbi connection failed")
	// },
	}
	uri.URL, _ = url.Parse(str)

	return uri
}

//
// func (uri *URI) checkScheme() (err error) {
// 	switch uri.Scheme {
// 	case schemeMemory, SchemeFile, SchemeTCP:
// 	default:
// 		err = ErrUnsupportedScheme
// 	}
// 	return err
// }

// Open opens the uri based on the scheme
//func (uri *URI) Open() (rw io.ReadWriteCloser, err error) {
//switch uri.Scheme {

//ase SchemeFile:
//uri.fh, err = uri.FileOpen(uri)

//case SchemeTCP:
//uri.fh, err = uri.TCPOpen(uri)

// default:
// 	err = ErrUnsupportedScheme
// }
//
// if err != nil {
//rw = uri.fh
// 	}
//
// 	return
// }

// ReadOnly returns whether or not the uri is meant for read-only operations.  By default
// all uri's are readonly.  It looks for the 'wr' param in the url and if present sets
// the uri to be writable
func (uri *URI) ReadOnly() bool {
	q := uri.Query()
	if _, ok := q["wr"]; ok {
		return false
	}
	return true
}
