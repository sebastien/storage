from storage import Types, DirectoryBackend, DBMBackend, Storable
from storage.objects import StoredObject, ObjectStorage
from storage.raw import StoredRaw, RawStorage
from storage.index import Indexing, Indexes
import base64


__doc__ = """\
A collection of objects that are used in many web sites and web applications.
This shows how to build the baseline.
"""

# -----------------------------------------------------------------------------
#
# FILE (RAW)
#
# -----------------------------------------------------------------------------


class File(StoredRaw):
    """Stores any kind of file"""


# -----------------------------------------------------------------------------
#
# IMAGE (RAW)
#
# -----------------------------------------------------------------------------


class Image(StoredRaw):
    """Stores image files"""

    def getFormat(self):
        return self.meta("format")

    def getPreview(self):
        meta = self.meta()
        if meta.get("preview"):
            return base64.decodestring(meta.get("preview"))
        else:
            return self.getFull()

    def hasPreview(self):
        return self.meta("preview")

    def setPreview(self, data):
        self.meta("preview", base64.b64encode(data))
        return self

    def getWidth(self):
        return self.meta("width")

    def getHeight(self):
        return self.meta("height")

    def getSize(self):
        return self.getWidth(), self.getHeight()

    def getFull(self):
        return "".join(self.data())

    def getURL(self):
        # FIXME: Should be done by storage.web
        return "api/image/%s/full" % (self.oid)

    def getPreviewURL(self):
        # FIXME: Should be done by storage.web
        return "api/image/%s/preview" % (self.oid)

    def export(self, **options):
        r = super(Image, self).export(**options)
        if not options.get("preview") and "preview" in r:
            del r["preview"]
        return r


# -----------------------------------------------------------------------------
#
# VIDEO (RAW)
#
# -----------------------------------------------------------------------------

# FIXME: Should have an export mode that by default does not include
# the preview (compact)
class Video(StoredRaw):

    FIELDS = dict(
        dimension=Types.TUPLE(Types.INTEGER, Types.INTEGER),
        duration=Types.INTEGER,
        audio=Types.MAP(),
        video=Types.MAP(),
        language=Types.STRING,
        snapshot=Types.STRING,
        subtitles=Types.LIST(Types.STRING),
        original=Types.STRING,
        format=Types.STRING,
        # LIST OF ALTERNATIVE FORMATS
        alternatives=Types.LIST(Types.STRING),
    )

    # ByOriginialFile = Index(lambda _:os.path.basename(_.meta().get("original")))
    # Index should support: add/create/update
    # They will also have to support a mapping of object id to index-key.
    # Ex:
    # - video was keyed at tricot.mp4
    # - video original is changed to tricot.ogv
    # - video has to be unkeyed from tricot.mp4, then keyed to tricot.ogv
    # So basically: Index.getKeyFor(objectOrOid)
    # and then: Index.add(storable)
    # and then: Index.update(storable)
    # and then: Index.remove(storable)
    # remove the changed
    # indexes will also have to be persistent

    def getPreview(self):
        thumbnail = self.getThumbnail()
        if thumbnail:
            return thumbnail.data()
        return None

    def getApiUrl(self):
        return "/api/video/%s/preview" % (self.oid)

    def hasPreview(self):
        return self.meta("thumbnail")

    def getThumbnail(self):
        return Photo.Get(self.meta("thumbnail"))

    def getSequence(self):
        return Photo.Get(self.meta("sequence"))

    def getOriginal(self):
        return Video.Get(self.meta("original"))

    def isOriginal(self):
        return self.meta("original") is None

    def getAlternatives(self):
        alternatives = self.meta("alternatives")
        return alternatives.values() if alternatives else ()

    def getAlternativeFormats(self):
        """Returns a map of `FORMAT`->`RID` listing the different
        formats for the given video. The original video is returned as
        `original` format, while the alternative videos are returned by
        their format.

        This method will always return the same value for the original
        video and its transcoded versions.
        """
        formats = {}
        if self.isOriginal():
            formats["original"] = self.oid
            for _ in self.getAlternatives():
                f = Video.Get(_).getFormat()
                formats[f] = _
            return formats
        else:
            return Video.Get(self.getOriginal()).getAlternativeFormats()

    def getAlternative(self, format):
        alternatives = self.meta("alternatives")
        if not alternatives:
            return None
        video_oid = alternatives.get(format)
        if not video_oid:
            return None
        else:
            return Video.Get(video_oid)

    def hasAlternative(self, format):
        alternatives = self.meta("alternatives")
        if not alternatives:
            return None
        return alternatives.has_key(format)

    def setAlternative(self, format, video):
        alternatives = self.meta("alternatives")
        if not alternatives:
            self.meta("alternatives", {(format): video.getID()})
        else:
            self.meta("alternatives")[format] = video.getID()
        # We copy the thumbnail/sequence, if any
        video.meta("thumbnail", self.meta("thumbnail"))
        video.meta("sequence", self.meta("sequence"))
        video.meta("original", self.getID())
        return self

    def getFormat(self):
        """Returns the format for this video (stored as container.format)"""
        return self.meta("container")["format"] if self.meta("container") else None


# -----------------------------------------------------------------------------
#
# ACCOUNT (OBJECT)
#
# -----------------------------------------------------------------------------

# FIXME: Should restrict: CREATE, READ, UPDATE, DELETE
# by default: CREATE=everyone, READ=everyone, UPDATE=owner, DELETE=owner
class Account(StoredObject):

    PROPERTIES = dict(
        email=Types.EMAIL,
        password=Types.STRING,
        name=Types.STRING,
        avatar=Types.STRING,
        roles=Types.LIST(Types.STRING),
        groups=Types.LIST,
        tokens=Types.MAP(),
    )

    INDEX_BY = dict(
        email=Indexing.Normalize,
        name=Indexing.Normalize,
    )

    def setPassword(self, password):
        self.password = PASSWORD_HASH.encrypt(password)

    def checkPassword(self, password):
        return PASSWORD_HASH.verify(password, self.password)


# -----------------------------------------------------------------------------
#
# COMMENT (OBJECT)
#
# -----------------------------------------------------------------------------


class Comment(StoredObject):

    PROPERTIES = dict(
        author=Types.REFERENCE(Account),
        date=Types.DATE,
        message=Types.HTML,
    )

    INDEX_BY = dict(keywords=lambda n, _: Indexing.Keywords((_.message, _.author)))

    def export(self, **options):
        exported = StoredObject.export(self, **options)
        if self.author:
            exported["author"] = self.author.export(profile="small")
        return exported


# -----------------------------------------------------------------------------
#
# COMMENT (OBJECT)
#
# -----------------------------------------------------------------------------


class Article(StoredObject):

    PROPERTIES = {
        "title": Types.STRING,
        "date": Types.STRING,
        "author": Types.STRING,
        "status": Types.STRING,
        "content": Types.STRING,
        "contentHTML": Types.STRING,
    }

    INDEX_BY = dict(
        date=Indexing.Normalize,
        keywords=lambda n, _: Indexing.Keywords(
            (_.status, _.title, _.author, _.content)
        ),
    )


# -----------------------------------------------------------------------------
#
# SITE (OBJECT)
#
# -----------------------------------------------------------------------------


class Site(StoredObject):
    """Stores site-configuration variables and objects"""

    PROPERTIES = dict()


class Interface:
    """Provides a single-point interface to all the data loading/saving/querying
    operations."""

    CLASSES = (File, Image, Video, Comment, Article, Account, Site)
    INSTANCE = None
    OBJECT_BACKEND = DirectoryBackend
    RAW_BACKEND = DirectoryBackend
    INDEX_BACKEND = DBMBackend

    @classmethod
    def Get(self):
        if not self.INSTANCE:
            self.INSTANCE = self()
        return self.INSTANCE

    def __init__(self, path="Data/", prefix="/api", readonly=False):
        self.objects = self._createObjectStorage(path)
        self.raw = self._createRawStorage(path)
        self.indexes = self._createIndexes(path)
        self.server = self._createStorageServer(prefix, readonly=readonly)

    def first(self, iterable, count=10):
        """A utiilty function to select the first 10 elements
        of an iterator."""
        while count > 0:
            try:
                yield iterable.next()
                count -= 1
            except StopIteration as e:
                break

    def _createObjectStorage(self, prefix):
        return ObjectStorage(self.OBJECT_BACKEND(prefix)).use(
            *[_ for _ in self.CLASSES if issubclass(_, StoredObject)]
        )

    def _createRawStorage(self, prefix):
        return RawStorage(self.RAW_BACKEND(prefix)).use(
            *[_ for _ in self.CLASSES if issubclass(_, StoredRaw)]
        )

    def _createIndexes(self, prefix):
        return Indexes(self.INDEX_BACKEND, prefix).use(
            *[_ for _ in self.CLASSES if issubclass(_, Storable)]
        )

    def sync(self):
        """Calls `sync()` on all the backends."""
        self.objects.sync()
        self.raw.sync()
        # FIXME: indexes have no sync?
        # self.indexes.sync()
        return True

    def reindex(self):
        """Rebuilds the index"""
        return self.indexes.rebuild(sync=True)


# EOF
