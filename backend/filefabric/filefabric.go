// Package filefabric provides an interface to Storage Made Easy's
// File Fabric storage system.
package filefabric

/*
Q is direct upload support good to implement? Or should it be optional?

Can't upload zero length files these upload as a single space. We set
these files to have the emptyMimeType and recognise that in Size() and
Open() to work around this.

FIXME oauth-like flow

FIXME don't know what goes in getFolderQuota so About doesn't work

Q What is the best way to read the total/used/free space available?

Failing tests
-------------

Caused by dir file /move time change

            --- FAIL: TestIntegration/FsMkdir/FsPutFiles/FsDirMove (316.52s)
            --- FAIL: TestIntegration/FsMkdir/FsPutFiles/ObjectModTime (0.43s)
            --- FAIL: TestIntegration/FsMkdir/FsPutFiles/FsIsFile (1.17s)

Unknown

            --- FAIL: TestIntegration/FsMkdir/FsPutFiles/FromRoot (21.33s)

Should be fixed

            --- FAIL: TestIntegration/FsMkdir/FsPutFiles/ObjectAbout (0.00s)


API limitations
---------------

Can't change the name of a file when copying which limits the
usefulness as can't copy a file to the same directory.

Can't set mime type in doUploadInit ("fi_contenttype")
- being ignored as Content-Type: in PUT also
- need to alter it afterwards which is less efficient

Not being able to stream uploads is unfortunate.

Bug in range requests: "Range: bytes=0-0" returns the whole file - it
should just return the first byte.

When copy/move file or folder with a file then `fi_localtime` of
copied/moved file can be changed on folder refresh.

// TestFileFabric
maxFileLength = 14094
*/

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/lib/atexit"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/random"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/backend/filefabric/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	minSleep      = 20 * time.Millisecond
	maxSleep      = 10 * time.Second
	decayConstant = 2                // bigger for slower decay, exponential
	listChunks    = 1000             // chunk size to read directory listings
	tokenLifeTime = 55 * time.Minute // 1 hour minus a bit of leeway
	defaultRootID = ""               // default root ID
	emptyMimeType = "application/vnd.rclone.empty.file"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "filefabric",
		Description: "File Fabric",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "url",
			Help:     "URL of File Fabric to connect to",
			Required: true,
			Examples: []fs.OptionExample{{
				Value: "https://yourfabric.smestorage.com",
				Help:  "Connect to yourfabric",
			}},
		}, {
			Name: "root_folder_id",
			Help: `ID of the root folder
Leave blank normally.

Fill in to make rclone start with directory of a given ID.
`,
		}, {
			Name: "permanent_token",
			Help: `Permanent Authentication Token

A Permanent Authentication Token can be created in the File Fabric, on
the users Dashboard under Security, there is an entry you'll see
called "My Authentication Tokens". Click the Manage button to create
one.

These tokens are normally valid for several years.
`,
		}, {
			Name: "token",
			Help: `Session Token

This is a session token which rclone caches in the config file. It is
usually valid for 1 hour.

Don't set this value - rclone will set it automatically.
`,
			Advanced: true,
		}, {
			Name: "token_expiry",
			Help: `Token expiry time

Don't set this value - rclone will set it automatically.
`,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.Display |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	URL            string               `config:"url"`
	RootFolderID   string               `config:"root_folder_id"`
	PermanentToken string               `config:"permanent_token"`
	Token          string               `config:"token"`
	TokenExpiry    string               `config:"token_expiry"`
	Enc            encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote filefabric
type Fs struct {
	name         string             // name of this remote
	root         string             // the path we are working on
	opt          Options            // parsed options
	features     *fs.Features       // optional features
	m            configmap.Mapper   // to save config
	srv          *rest.Client       // the connection to the one drive server
	dirCache     *dircache.DirCache // Map of directory path to directory id
	pacer        *fs.Pacer          // pacer for API calls
	tokenMu      sync.Mutex         // hold when reading the token
	token        string             // current access token
	tokenExpiry  time.Time          // time the current token expires
	tokenExpired int32              // read and written with atomic
}

// Object describes a filefabric object
//
// Will definitely have info but maybe not meta
type Object struct {
	fs          *Fs       // what this object is part of
	remote      string    // The remote path
	hasMetaData bool      // whether info below has been set
	size        int64     // size of the object
	modTime     time.Time // modification time of the object
	id          string    // ID of the object
	contentType string    // ContentType of object
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("filefabric root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// parsePath parses a filefabric 'url'
func parsePath(path string) (root string) {
	root = strings.Trim(path, "/")
	return
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// Retry any of these
var retryStatusCodes = []struct {
	code  string
	sleep time.Duration
}{
	{
		// Can not create folder now. We are not able to complete the
		// requested operation with such name. We are processing
		// delete in that folder. Please try again later or use
		// another name. (error_background)
		code:  "error_background",
		sleep: 6 * time.Second,
	},
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func (f *Fs) shouldRetry(resp *http.Response, err error, status api.OKError) (bool, error) {
	if err != nil {
		return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
	}
	if status != nil && !status.OK() {
		err = status // return the error from the RPC
		code := status.GetCode()
		if code == "login_token_expired" {
			atomic.AddInt32(&f.tokenExpired, 1)
		} else {
			for _, retryCode := range retryStatusCodes {
				if code == retryCode.code {
					if retryCode.sleep > 0 {
						// make this thread only sleep extra time
						fs.Debugf(f, "Sleeping for %v to wait for %q error to clear", retryCode.sleep, retryCode.code)
						time.Sleep(retryCode.sleep)
					}
					return true, err
				}
			}
		}
	}
	return false, err
}

// readMetaDataForPath reads the metadata from the path
func (f *Fs) readMetaDataForPath(ctx context.Context, rootID string, path string) (info *api.Item, err error) {
	var resp api.FileResponse
	_, err = f.rpc(ctx, "checkPathExists", params{
		"path": f.opt.Enc.FromStandardPath(path),
		"pid":  rootID,
	}, &resp, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to check path exists")
	}
	if resp.Exists != "y" {
		return nil, fs.ErrorObjectNotFound
	}
	return &resp.Item, nil

	/*
		// defer fs.Trace(f, "path=%q", path)("info=%+v, err=%v", &info, &err)
		leaf, directoryID, err := f.dirCache.FindPath(ctx, path, false)
		if err != nil {
			if err == fs.ErrorDirNotFound {
				return nil, fs.ErrorObjectNotFound
			}
			return nil, err
		}

		found, err := f.listAll(ctx, directoryID, false, true, func(item *api.Item) bool {
			if item.Name == leaf {
				info = item
				return true
			}
			return false
		})
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fs.ErrorObjectNotFound
		}
		return info, nil
	*/
}

// Gets the token or gets a new one if necessary
func (f *Fs) getToken(ctx context.Context) (token string, err error) {
	f.tokenMu.Lock()
	var refreshed = false
	defer func() {
		if refreshed {
			atomic.StoreInt32(&f.tokenExpired, 0)
		}
		f.tokenMu.Unlock()
	}()

	expired := atomic.LoadInt32(&f.tokenExpired) != 0
	if expired {
		fs.Debugf(f, "Token invalid - refreshing")
	}
	if f.token == "" {
		fs.Debugf(f, "Empty token - refreshing")
		expired = true
	}
	now := time.Now()
	if f.tokenExpiry.IsZero() || now.After(f.tokenExpiry) {
		fs.Debugf(f, "Token expired - refreshing")
		expired = true
	}
	if !expired {
		return f.token, nil
	}

	var info api.GetTokenByAuthTokenResponse
	_, err = f.rpc(ctx, "getTokenByAuthToken", params{
		"token":     "*",
		"authtoken": f.opt.PermanentToken,
	}, &info, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to get session token")
	}
	refreshed = true
	now = now.Add(tokenLifeTime)
	f.token = info.Token
	f.tokenExpiry = now
	f.m.Set("token", f.token)
	f.m.Set("token_expiry", now.Format(time.RFC3339))
	return f.token, nil
}

// params for rpc
type params map[string]interface{}

// rpc calls the rpc.php method of the SME file fabric
//
// This is an entry point to all the method calls
//
// If result is nil then resp.Body will need closing
func (f *Fs) rpc(ctx context.Context, function string, p params, result api.OKError, options []fs.OpenOption) (resp *http.Response, err error) {
	defer log.Trace(f, "%s(%+v) options=%+v", function, p, options)("result=%+v, err=%v", &result, &err)

	// Get the token from params if present otherwise call getToken
	var token string
	if tokenI, ok := p["token"]; !ok {
		token, err = f.getToken(ctx)
		if err != nil {
			return resp, err
		}
	} else {
		token = tokenI.(string)
	}
	var data = url.Values{
		"function":  {function},
		"token":     {token},
		"apiformat": {"json"},
	}
	for k, v := range p {
		data.Set(k, fmt.Sprint(v))
	}
	opts := rest.Opts{
		Method:      "POST",
		Path:        "/api/rpc.php",
		ContentType: "application/x-www-form-urlencoded",
		Options:     options,
	}
	err = f.pacer.Call(func() (bool, error) {
		// Refresh the body each retry
		opts.Body = strings.NewReader(data.Encode())
		resp, err = f.srv.CallJSON(ctx, &opts, nil, result)
		return f.shouldRetry(resp, err, result)
	})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// NewFs constructs an Fs from the path, container:path
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
	ctx := context.Background()

	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	opt.URL = strings.TrimSuffix(opt.URL, "/")
	if opt.URL == "" {
		return nil, errors.New("url must be set")
	}

	tokenExpiry, err := time.Parse(time.RFC3339, opt.TokenExpiry)
	if err != nil {
		fs.Errorf(nil, "Failed to parse token_expiry option: %v", err)
	}

	root = parsePath(root)

	client := fshttp.NewClient(fs.Config)

	f := &Fs{
		name:        name,
		root:        root,
		opt:         *opt,
		m:           m,
		srv:         rest.NewClient(client).SetRoot(opt.URL),
		pacer:       fs.NewPacer(pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		token:       opt.Token,
		tokenExpiry: tokenExpiry,
	}
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
	}).Fill(f)

	if opt.RootFolderID == "" {
		opt.RootFolderID = defaultRootID
	}

	f.dirCache = dircache.New(f.root, opt.RootFolderID, f)

	// Find out whether the root is a file or a directory or doesn't exist
	var errReturn error
	if f.root != "" {
		info, err := f.readMetaDataForPath(ctx, f.opt.RootFolderID, f.root)
		if err == nil && info != nil {
			if info.Type == api.ItemTypeFile {
				// Root is a file
				// Point the root to the parent directory
				f.root, _ = dircache.SplitPath(root)
				f.dirCache = dircache.New(f.root, opt.RootFolderID, f)
				errReturn = fs.ErrorIsFile
				// Cache the ID of the parent of the file as the root ID
				f.dirCache.Put(f.root, info.PID)
			} else if info.Type == api.ItemTypeFolder {
				// Root is a dir - cache its ID
				f.dirCache.Put(f.root, info.ID)
			}
		} else {
			// Root is not found so a directory
		}
	}
	return f, errReturn
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.Item) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	var err error
	if info != nil {
		// Set info
		err = o.setMetaData(info)
	} else {
		err = o.readMetaData(ctx) // reads info and meta, returning an error
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	// Find the leaf in pathID
	found, err = f.listAll(ctx, pathID, true, false, func(item *api.Item) bool {
		if item.Name == leaf {
			pathIDOut = item.ID
			return true
		}
		return false
	})
	return pathIDOut, found, err
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	//fs.Debugf(f, "CreateDir(%q, %q)\n", pathID, leaf)
	var info api.DoCreateNewFolderResponse
	_, err = f.rpc(ctx, "doCreateNewFolder", params{
		"fi_pid":  pathID,
		"fi_name": f.opt.Enc.FromStandardName(leaf),
	}, &info, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create directory")
	}
	// fmt.Printf("...Id %q\n", *info.Id)
	return info.Item.ID, nil
}

// list the objects into the function supplied
//
// If directories is set it only sends directories
// User function to process a File item from listAll
//
// Should return true to finish processing
type listAllFn func(*api.Item) bool

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
func (f *Fs) listAll(ctx context.Context, dirID string, directoriesOnly bool, filesOnly bool, fn listAllFn) (found bool, err error) {
	var (
		p = params{
			"fi_pid":     dirID,
			"count":      listChunks,
			"subfolders": "y",
			// Cut down the things that are returned
			"options": "filelist|" + api.ItemFields,
		}
		n = 0
	)
OUTER:
	for {
		var info api.GetFolderContentsResponse
		_, err = f.rpc(ctx, "getFolderContents", p, &info, nil)
		if err != nil {
			return false, errors.Wrap(err, "failed to list directory")
		}
		for i := range info.Items {
			item := &info.Items[i]
			if item.Type == api.ItemTypeFolder {
				if filesOnly {
					continue
				}
			} else if item.Type == api.ItemTypeFile {
				if directoriesOnly {
					continue
				}
			} else {
				fs.Debugf(f, "Ignoring %q - unknown type %q", item.Name, item.Type)
				continue
			}
			if item.Trash {
				continue
			}
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(item) {
				found = true
				break OUTER
			}
		}
		n += len(info.Items)
		if n >= info.Total {
			break
		}
		p["from"] = n
	}

	return found, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	var iErr error
	_, err = f.listAll(ctx, directoryID, false, false, func(info *api.Item) bool {
		remote := path.Join(dir, info.Name)
		if info.Type == api.ItemTypeFolder {
			// cache the directory ID for later lookups
			f.dirCache.Put(remote, info.ID)
			d := fs.NewDir(remote, time.Time(info.Modified)).SetID(info.ID).SetItems(info.SubFolders)
			entries = append(entries, d)
		} else if info.Type == api.ItemTypeFile {
			o, err := f.newObjectWithInfo(ctx, remote, info)
			if err != nil {
				iErr = err
				return true
			}
			entries = append(entries, o)
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if iErr != nil {
		return nil, iErr
	}
	return entries, nil
}

// Creates from the parameters passed in a half finished Object which
// must have setMetaData called on it
//
// Returns the object, leaf, directoryID and error
//
// Used to create new objects
func (f *Fs) createObject(ctx context.Context, remote string, modTime time.Time, size int64) (o *Object, leaf string, directoryID string, err error) {
	// Create the directory for the object if it doesn't exist
	leaf, directoryID, err = f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return
	}
	// Temporary Object under construction
	o = &Object{
		fs:     f,
		remote: remote,
	}
	return o, leaf, directoryID, nil
}

// Put the object
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	size := src.Size()
	modTime := src.ModTime(ctx)

	o, _, _, err := f.createObject(ctx, remote, modTime, size)
	if err != nil {
		return nil, err
	}
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// deleteObject removes an object by ID
func (f *Fs) deleteObject(ctx context.Context, id string) (err error) {
	var info api.DeleteResponse
	_, err = f.rpc(ctx, "doDeleteFile", params{
		"fi_id":            id,
		"completedeletion": "n",
	}, &info, nil)
	if err != nil {
		return errors.Wrap(err, "failed to delete file")
	}
	return nil
}

// purgeCheck removes the root directory, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	root := path.Join(f.root, dir)
	if root == "" {
		return errors.New("can't purge root directory")
	}
	dc := f.dirCache
	rootID, err := dc.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	if check {
		found, err := f.listAll(ctx, rootID, false, false, func(item *api.Item) bool {
			fs.Debugf(dir, "Rmdir: contains file: %q", item.Name)
			return true
		})
		if err != nil {
			return err
		}
		if found {
			return fs.ErrorDirectoryNotEmpty
		}
	}

	var info api.EmptyResponse
	_, err = f.rpc(ctx, "doDeleteFolder", params{
		"fi_id": rootID,
	}, &info, nil)
	f.dirCache.FlushDir(dir)
	if err != nil {
		return errors.Wrap(err, "failed to remove directory")
	}
	return nil
}

// Rmdir deletes the root folder
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	//fs.Debugf(f, "CreateDir(%q, %q)\n", pathID, leaf)
	return f.purgeCheck(ctx, dir, true)
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	err := srcObj.readMetaData(ctx)
	if err != nil {
		return nil, err
	}

	// Create temporary object
	dstObj, leaf, directoryID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
	if err != nil {
		return nil, err
	}

	if leaf != path.Base(srcObj.remote) {
		fs.Debugf(src, "Can't copy - can't change the name of files")
		return nil, fs.ErrorCantCopy
	}

	// Copy the object
	var info api.FileResponse
	_, err = f.rpc(ctx, "doCopyFile", params{
		"fi_id":  srcObj.id,
		"fi_pid": directoryID,
		"force":  "y",
		//"fi_name": f.opt.Enc.FromStandardName(leaf),
	}, &info, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to copy file")
	}
	err = dstObj.setMetaData(&info.Item)
	if err != nil {
		return nil, err
	}
	return dstObj, nil
}

// Purge deletes all the files and the container
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, false)
}

// Rename the leaf of a file or directory in a directory
func (f *Fs) renameLeaf(ctx context.Context, id string, newLeaf string) (item *api.Item, err error) {
	var info api.FileResponse
	_, err = f.rpc(ctx, "doRenameFile", params{
		"fi_id":   id,
		"fi_name": newLeaf,
	}, &info, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to rename leaf")
	}
	return &info.Item, nil
}

// move a file or folder
//
// This is complicated by the fact that there is an API to move files
// between directories and a separate one to rename them.  We try to
// call the minimum number of API calls.
func (f *Fs) move(ctx context.Context, id, oldLeaf, newLeaf, oldDirectoryID, newDirectoryID string) (item *api.Item, err error) {
	newLeaf = f.opt.Enc.FromStandardName(newLeaf)
	oldLeaf = f.opt.Enc.FromStandardName(oldLeaf)
	doRenameLeaf := oldLeaf != newLeaf
	doMove := oldDirectoryID != newDirectoryID

	// Now rename the leaf to a temporary name if we are moving to
	// another directory to make sure we don't overwrite something
	// in the destination directory by accident
	if doRenameLeaf && doMove {
		tmpLeaf := newLeaf + "." + random.String(8)
		item, err = f.renameLeaf(ctx, id, tmpLeaf)
		if err != nil {
			return nil, err
		}
	}

	// Move the object to a new directory (with the existing name)
	// if required
	if doMove {
		var info api.MoveFilesResponse
		_, err = f.rpc(ctx, "doMoveFiles", params{
			"fi_ids": id,
			"dir_id": newDirectoryID,
		}, &info, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to move file to new directory")
		}
		item = &info.Item
	}

	// Rename the leaf to its final name if required
	if doRenameLeaf {
		item, err = f.renameLeaf(ctx, id, newLeaf)
		if err != nil {
			return nil, err
		}
	}

	return item, nil
}

// Move src to this remote using server side move operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	// find the source directoryID
	srcLeaf, srcDirectoryID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		return nil, err
	}

	// Create temporary object
	dstObj, dstLeaf, dstDirectoryID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
	if err != nil {
		return nil, err
	}

	// Do the move
	item, err := f.move(ctx, srcObj.id, srcLeaf, dstLeaf, srcDirectoryID, dstDirectoryID)
	if err != nil {
		return nil, err
	}

	// Set the metadata from what was returned or read it fresh
	if item == nil {
		err = dstObj.readMetaData(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		err = dstObj.setMetaData(item)
		if err != nil {
			return nil, err
		}
	}
	return dstObj, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcID, srcDirectoryID, srcLeaf, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	// Do the move
	_, err = f.move(ctx, srcID, srcLeaf, dstLeaf, srcDirectoryID, dstDirectoryID)
	if err != nil {
		return err
	}
	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// About gets quota information
func (f *Fs) _FIXMEAbout(ctx context.Context) (usage *fs.Usage, err error) {
	/*
		var info api.EmptyResponse
		_, err = f.rpc(ctx, "getFolderQuota", params{
			"fi_id": f.opt.RootFolderID,
		}, &info, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read quota info")
		}

		// FIXME don't know what the result should look like!

		usage = &fs.Usage{
			// Used:  fs.NewUsageValue(user.SpaceUsed),                    // bytes in use
			// Total: fs.NewUsageValue(user.SpaceAmount),                  // bytes total
			// Free:  fs.NewUsageValue(user.SpaceAmount - user.SpaceUsed), // bytes free
		}
	*/
	return usage, nil
}

// CleanUp empties the trash
func (f *Fs) CleanUp(ctx context.Context) (err error) {
	var info api.EmptyResponse
	_, err = f.rpc(ctx, "emptyTrashInBackground", params{}, &info, nil)
	if err != nil {
		return errors.Wrap(err, "failed to empty trash")
	}
	return nil
}

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash of the object in the requested format as a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	err := o.readMetaData(context.TODO())
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return 0
	}
	if o.contentType == emptyMimeType {
		return 0
	}
	return o.size
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.Item) (err error) {
	if info.Type != api.ItemTypeFile {
		return errors.Wrapf(fs.ErrorNotAFile, "%q is %q", o.remote, info.Type)
	}
	o.hasMetaData = true
	o.size = info.Size
	o.modTime = time.Time(info.Modified)
	if !time.Time(info.LocalTime).IsZero() {
		o.modTime = time.Time(info.LocalTime)
	}
	o.id = info.ID
	o.contentType = info.ContentType
	return nil
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData(ctx context.Context) (err error) {
	if o.hasMetaData {
		return nil
	}
	rootID, err := o.fs.dirCache.RootID(ctx, false)
	if err != nil {
		return err
	}
	info, err := o.fs.readMetaDataForPath(ctx, rootID, o.remote)
	if err != nil {
		if apiErr, ok := err.(*api.Status); ok {
			if apiErr.Code == "not_found" || apiErr.Code == "trashed" {
				return fs.ErrorObjectNotFound
			}
		}
		return err
	}
	return o.setMetaData(info)
}

// ModTime returns the modification time of the object
//
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	err := o.readMetaData(ctx)
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	return o.modTime
}

// modifyFile updates file metadata
//
// keyValues should be key, value pairs
func (o *Object) modifyFile(ctx context.Context, keyValues [][2]string) error {
	var info api.FileResponse
	var data strings.Builder
	for _, keyValue := range keyValues {
		data.WriteString(keyValue[0])
		data.WriteRune('=')
		data.WriteString(keyValue[1])
		data.WriteRune('\n')
	}
	_, err := o.fs.rpc(ctx, "doModifyFile", params{
		"fi_id": o.id,
		"data":  data.String(),
	}, &info, nil)
	if err != nil {
		return errors.Wrap(err, "failed to update metadata")
	}
	return o.setMetaData(&info.Item)
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return o.modifyFile(ctx, [][2]string{
		{"fi_localtime", api.Time(modTime).String()},
	})
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	if o.id == "" {
		return nil, errors.New("can't download - no id")
	}
	if o.contentType == emptyMimeType {
		return ioutil.NopCloser(bytes.NewReader([]byte{})), nil
	}
	fs.FixRangeOption(options, o.size)
	resp, err := o.fs.rpc(ctx, "getFile", params{
		"fi_id": o.id,
	}, nil, options)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	modTime := src.ModTime(ctx)
	remote := o.remote
	size := src.Size()

	// Can't upload 0 length files - these upload as a single
	// space.
	// if size == 0 {
	// 	return fs.ErrorCantUploadEmptyFiles
	// }

	// Create the directory for the object if it doesn't exist
	leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return err
	}

	// Initialise the upload
	var upload api.DoInitUploadResponse
	timestamp := api.Time(modTime).String()
	encodedLeaf := o.fs.opt.Enc.FromStandardName(leaf)
	base64EncodedLeaf := base64.StdEncoding.EncodeToString([]byte(encodedLeaf))
	contentType := fs.MimeType(ctx, src)
	if size == 0 {
		contentType = emptyMimeType
	}
	p := params{
		"fi_name":             encodedLeaf,
		"fi_pid":              directoryID,
		"fi_filename":         encodedLeaf,
		"fi_localtime":        timestamp,
		"fi_modified":         timestamp,
		"fi_contenttype":      contentType,
		"responsetype":        "json", // make the upload.cgi return JSON
		"directuploadsupport": "n",    // FIXME should we support this?
		// "chunkifbig": "n",	    // FIXME multipart?
	}
	// Set the size if known
	if size >= 0 {
		p["fi_size"] = size
	}
	_, err = o.fs.rpc(ctx, "doInitUpload", p, &upload, nil)
	if err != nil {
		return errors.Wrap(err, "failed to initialize upload")
	}

	// Cancel the upload if aborted or it fails
	finalized := false
	defer atexit.OnError(&err, func() {
		if finalized {
			return
		}
		fs.Debugf(o, "Cancelling upload %s", upload.UploadCode)
		var cancel api.EmptyResponse
		_, fErr := o.fs.rpc(ctx, "doAbortUpload", params{
			"uploadcode": upload.UploadCode,
		}, &cancel, nil)
		if fErr != nil {
			fs.Errorf(o, "failed to cancel upload: %v", fErr)
		}
	})()

	// Post the file with the upload code
	var uploader api.UploaderResponse
	opts := rest.Opts{
		//Method: "POST",
		Method:      "PUT",
		Path:        "/cgi-bin/uploader/uploader1.cgi/" + base64EncodedLeaf + "?" + upload.UploadCode,
		Body:        in,
		ContentType: contentType,
		// MultipartParams:      url.Values{},
		// MultipartContentName: "file",
		// MultipartFileName:    "datafile",
	}
	// Set the size if known
	if size >= 0 {
		var contentLength = size
		opts.ContentLength = &contentLength // NB CallJSON scribbles on this which is naughty
	}
	err = o.fs.pacer.CallNoRetry(func() (bool, error) {
		resp, err := o.fs.srv.CallJSON(ctx, &opts, nil, &uploader)
		return o.fs.shouldRetry(resp, err, nil)
	})
	if err != nil {
		return errors.Wrap(err, "failed to upload")
	}
	if uploader.Success != "y" {
		return errors.Errorf("upload failed")
	}
	if size > 0 && uploader.FileSize != size {
		return errors.Errorf("upload failed: size mismatch: want %d got %d", size, uploader.FileSize)
	}

	// Now finalize the file
	var finalize api.DoCompleteUploadResponse
	p = params{
		"uploadcode": upload.UploadCode,
		"remotetime": timestamp,
		"fi_size":    uploader.FileSize,
	}
	_, err = o.fs.rpc(ctx, "doCompleteUpload", p, &finalize, nil)
	if err != nil {
		return errors.Wrap(err, "failed to finalize upload")
	}
	finalized = true

	err = o.setMetaData(&finalize.File)
	if err != nil {
		return err
	}

	// Make sure content type is correct
	if o.contentType != contentType {
		fs.Debugf(o, "Correcting mime type from %q to %q", o.contentType, contentType)
		return o.modifyFile(ctx, [][2]string{
			{"fi_contenttype", contentType},
		})
	}

	return nil
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteObject(ctx, o.id)
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// MimeType returns the content type of the Object if
// known, or "" if not
func (o *Object) MimeType(ctx context.Context) string {
	return o.contentType
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Purger = (*Fs)(nil)
	_ fs.Copier = (*Fs)(nil)
	//_ fs.Abouter         = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.CleanUpper      = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.MimeTyper       = (*Object)(nil)
)
