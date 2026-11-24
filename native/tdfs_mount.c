#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

/* Keep libc structures on this side of the ABI. The managed callback returns Linux errno or a result. */
enum operation { ATTR, LIST, OPEN, CREATE, READ, WRITE, FLUSH, RELEASE, MKDIR, UNLINK, RMDIR, RENAME, TRUNCATE };
struct metadata { int64_t directory, size, modified, readonly; };
struct listing { void *buf; fuse_fill_dir_t fill; };
typedef int64_t (*handler)(int, const char *, const char *, uint64_t, int64_t, void *, int);
struct context { handler call; struct fuse *fs; };
static struct context *ctx(void) { return fuse_get_context()->private_data; }

static int attributes(const char *path, struct stat *s, struct fuse_file_info *fi) {
    struct metadata m = {0};
    int rc = ctx()->call(ATTR,path,NULL,fi ? fi->fh : 0,0,&m,0);
    if (rc < 0) return rc;
    memset(s,0,sizeof(*s));
    s->st_uid = s->st_gid = 1654;
    s->st_mode = m.directory ? S_IFDIR | (m.readonly ? 0555 : 0770) : S_IFREG | (m.readonly ? 0444 : 0660);
    s->st_nlink = m.directory ? 2 : 1;
    s->st_size = m.size;
    s->st_blocks = (m.size + 511) / 512;
    s->st_blksize = 4096;
    s->st_atime = s->st_mtime = s->st_ctime = m.modified;
    return 0;
}
int mesh_fill(void *state, const char *name, int64_t next) {
    struct listing *l = state;
    return l->fill(l->buf,name,NULL,next,0);
}
static int list(const char *path, void *buf, fuse_fill_dir_t fill, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    (void)fi; (void)flags;
    struct listing l = {buf,fill};
    return ctx()->call(LIST,path,NULL,0,off,&l,0);
}
static int file_open(int op, const char *path, struct fuse_file_info *fi) {
    int64_t v = ctx()->call(op,path,NULL,0,fi->flags,NULL,0);
    if (v < 0) return (int)v;
    fi->fh = (uint64_t)v;
    return 0;
}
static int open_file(const char *p, struct fuse_file_info *f) { return file_open(OPEN,p,f); }
static int create_file(const char *p, mode_t mode, struct fuse_file_info *f) { (void)mode; return file_open(CREATE,p,f); }
static int read_file(const char *p,char *b,size_t n,off_t off,struct fuse_file_info *f) {
    return ctx()->call(READ,p,NULL,f->fh,off,b,(int)n);
}
static int write_file(const char *p,const char *b,size_t n,off_t off,struct fuse_file_info *f) {
    return ctx()->call(WRITE,p,NULL,f->fh,off,(void*)b,(int)n);
}
static int flush_file(const char *p,struct fuse_file_info *f) { return ctx()->call(FLUSH,p,NULL,f->fh,0,NULL,0); }
static int sync_file(const char *p,int data,struct fuse_file_info *f) { (void)data; return flush_file(p,f); }
static int release_file(const char *p,struct fuse_file_info *f) { return ctx()->call(RELEASE,p,NULL,f->fh,0,NULL,0); }
static int make_dir(const char *p, mode_t mode) { (void)mode; return ctx()->call(MKDIR,p,NULL,0,0,NULL,0); }
static int unlink_file(const char *p) { return ctx()->call(UNLINK,p,NULL,0,0,NULL,0); }
static int remove_dir(const char *p) { return ctx()->call(RMDIR,p,NULL,0,0,NULL,0); }
static int rename_path(const char *a,const char *b,unsigned int flags) {
    if (flags > 1) return -EINVAL;
    return ctx()->call(RENAME,a,b,0,flags,NULL,0);
}
static int truncate_file(const char *p,off_t len,struct fuse_file_info *f) {
    return ctx()->call(TRUNCATE,p,NULL,f ? f->fh : 0,len,NULL,0);
}
static const struct fuse_operations ops = {
    .getattr=attributes,.readdir=list,.open=open_file,.create=create_file,.read=read_file,.write=write_file,
    .flush=flush_file,.fsync=sync_file,.release=release_file,.mkdir=make_dir,.unlink=unlink_file,.rmdir=remove_dir,
    .rename=rename_path,.truncate=truncate_file
};

void *mesh_mount(const char *path, handler call) {
    char *args[] = { "dotnetmesh", "-o", "subtype=dotnetmesh,fsname=tedd-dotnetmesh,allow_other,default_permissions,attr_timeout=0,entry_timeout=0,negative_timeout=0" };
    struct fuse_args config = FUSE_ARGS_INIT(3,args);
    struct context *c = calloc(1,sizeof(*c));
    if (!c) return NULL;
    c->call = call;
    c->fs = fuse_new(&config,&ops,sizeof(ops),c);
    fuse_opt_free_args(&config);
    if (!c->fs || fuse_mount(c->fs,path) != 0) {
        if (c->fs) fuse_destroy(c->fs);
        free(c);
        return NULL;
    }
    return c;
}
int mesh_loop(struct context *c) { return fuse_loop(c->fs); }
void mesh_stop(struct context *c) { fuse_exit(c->fs); fuse_unmount(c->fs); }
void mesh_destroy(struct context *c) { fuse_destroy(c->fs); free(c); }
