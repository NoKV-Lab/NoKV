"""Exceptions the NoKV Python SDK raises with their recovery attributes.

Every exception a ``Client`` call raises carries ``error_class`` and
``next_action``, the failure contract of the native CLI
(docs/cli-contract.md). Branch on them, never on the message.
"""

from __future__ import annotations


class AppendError(RuntimeError):
    """An append failed or needs recovery under its original operation identity.

    ``error_class`` says what the failure means for durable state and
    ``next_action`` what to do next: ``invalid_input`` with ``fix_input`` for
    an argument the call rejected, ``outcome_unknown`` with ``query_same`` for
    a reply that may follow publication, ``in_progress`` with ``poll`` for a
    running attempt, ``identity_conflict`` with ``do_not_retry`` for an
    identity already used for other inputs. ``query_same`` means: query
    ``Client.operation_status`` with the same ``operation_id`` and follow its
    ``next_action``. ``state`` is the observed durable operation state, or
    ``None`` when it is unknown; neither a timeout nor an unknown state proves
    that nothing was published, so never generate a replacement identity to
    bypass an unresolved operation. ``AppendCleanupUnresolved`` supplies
    ``retry_same_cleanup`` and an ``expected_state_digest``: pass that same
    digest to ``operation_recover`` after an uncertain reply. An already
    observed ``recovery_receipt`` and its publication identity are retained
    even when the later status query fails. Without an observed receipt, the
    publication identity remains ``None``. ``cause_code`` preserves the
    underlying RPC code, for example ``RequestReplayMismatch``, or ``None``
    when there is no RPC failure. ``retryable`` is always false: generic
    retry handlers must not turn an unresolved operation into a new action.
    The defaults describe an append whose outcome is unknown.
    """

    __module__ = "nokv"

    def __init__(
        self,
        message: str,
        operation_id: str,
        state: str | None,
        code: str,
        expected: str | None,
        cause_code: str | None = None,
        next_action: str = "query_same",
        publication_operation_id: str | None = None,
        expected_state_digest: str | None = None,
        recovery_receipt: dict | None = None,
        error_class: str = "outcome_unknown",
    ) -> None:
        super().__init__(message)
        self.operation_id = operation_id
        self.state = state
        self.code = code
        self.expected = expected
        self.cause_code = cause_code
        self.retryable = False
        self.error_class = error_class
        self.next_action = next_action
        self.publication_operation_id = publication_operation_id
        self.expected_state_digest = expected_state_digest
        self.recovery_receipt = recovery_receipt


class WorkspaceIncarnationMismatch(RuntimeError):
    """A publish was refused because the workbench is not the expected incarnation.

    Raised by ``Client.publish_bytes``, ``Client.publish_file`` and
    ``Client.put_bytes`` when ``expected_workspace_incarnation_id`` was given
    and the owner found the workbench bound to a different incarnation. The
    owner evaluates the fence atomically with ``expected_generation`` before
    any durable row or object exists, so nothing was written: the failure is
    ``precondition_failed`` with ``reread_then_retry``. ``expected`` is the
    fence the caller sent; read the current incarnation back
    (``find_workspaces`` or ``read`` metadata) before deciding whether to
    retry. ``Client.append_bytes`` raises it with the recovery attributes of
    ``AppendError`` and the class of its unresolved append.
    """

    __module__ = "nokv"

    def __init__(
        self,
        message: str,
        expected: str,
        error_class: str = "precondition_failed",
        next_action: str = "reread_then_retry",
    ) -> None:
        super().__init__(message)
        self.expected = expected
        self.error_class = error_class
        self.next_action = next_action
