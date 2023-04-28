TODO: Rework the FILE/RAW interface TODO: Add worker to sync TODO: Add
observer to observe changes to the filesystem in DirectoryBackend

Should do:

- Each object has a revision number 16bit is enough
- Each object has a mtime
- The object state is characterized by (rev, mtime) which allows to
  resolve conflicts
- Optionally, objects can carry their history of modifications in the
  form of alist like \[(rev, mtime), {attribute:value}, \[attribute\]\]
- Objects when importing an object, we look at the revision number and
  return the latest, unless it is explicit that we want to merge (ex:
  user saves, but object has changed... so user should be prompted what
  to do)

Cababilities:

- Files: serve file objects for content
- Filesystem: can give a path for a given data
- ObjectsOpt: object-specific optimizations
- MetricsOpt: metrics-specific optimizaitons
- RawOpt: raw-specific optimizations
- IndexOpt: index-specific optimizations
- Index: can store indexes TODO: What about limits?
