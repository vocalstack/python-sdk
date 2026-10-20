"""VocalStack speech API SDK (asyncio): one client, ``VocalStack``, grouped
by resource, with models generated from the API's one contract."""

from . import _models as models
from ._models import (
    Broadcast,
    Capabilities,
    CommittedSegment,
    CreateSessionResponse,
    DraftSegment,
    Health,
    PublishGrant,
    Recording,
    SearchResults,
    ServerFrame,
    SessionOptions,
    SessionStats,
    Transcription,
    TranscriptionJob,
    TranscriptCommitted,
    TranscriptDraft,
    TranscriptProgress,
    TranscriptSnapshot,
    TranscriptWords,
    Word,
)
from ._http import DEFAULT_BASE_URL
from .client import VocalStack
from .errors import VocalStackError
from .realtime import RealtimeTranscript
from .realtime import RealtimeSession, pace_realtime

__version__ = "1.0.0a1"

__all__ = [
    "DEFAULT_BASE_URL",
    "Broadcast",
    "Capabilities",
    "CommittedSegment",
    "CreateSessionResponse",
    "DraftSegment",
    "Health",
    "PublishGrant",
    "RealtimeTranscript",
    "Recording",
    "SearchResults",
    "ServerFrame",
    "SessionOptions",
    "SessionStats",
    "RealtimeSession",
    "Transcription",
    "TranscriptionJob",
    "TranscriptCommitted",
    "TranscriptDraft",
    "TranscriptProgress",
    "TranscriptSnapshot",
    "TranscriptWords",
    "VocalStack",
    "VocalStackError",
    "Word",
    "models",
    "pace_realtime",
]
