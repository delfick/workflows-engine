import threading

import attrs
import pytest

from computations import errors, jobs, protocols, state, storage


class M_State(state.State):
    pass


B_JobTracker = protocols.JobTracker[protocols.JobStatus[M_State]]

M_Result = state.Result[M_State]
M_StoredInfo = state.StoredInfo[M_State]
M_Storage = storage.MemoryStorage[M_State]
M_Workflow = jobs.WorkflowBase[M_State, protocols.Computation[M_State]]


@attrs.define
class Workflow:
    one: str
    count: int

    def for_storage(
        self,
        identifier: protocols.WorkflowIdentifier,
        job_tracker: B_JobTracker | None = None,
        /,
    ) -> protocols.WorkflowInformation:
        self.count += 1
        return state.WorkflowInformation(
            workflow_code="test_workflow",
            workflow_version=1,
            information={"one": self.one, "count": self.count},
        )


class TestMemoryStorage:
    def test_can_store_and_retrieve_and_update_a_new_workflow(self) -> None:
        store = M_Storage()

        workflow_one = Workflow(one="one", count=1)
        workflow_two = Workflow(one="two", count=5)

        identifier_one = store.store_new_workflow(workflow_one)
        identifier_two = store.store_new_workflow(workflow_two)

        assert identifier_one.identifier != identifier_two.identifier

        assert store.retrieve_workflow_information(
            identifier=identifier_one
        ) == state.WorkflowInformation(
            workflow_code="test_workflow",
            workflow_version=1,
            information={"one": "one", "count": 2},
        )
        assert store.retrieve_workflow_information(
            identifier=identifier_two
        ) == state.WorkflowInformation(
            workflow_code="test_workflow",
            workflow_version=1,
            information={"one": "two", "count": 6},
        )

        store.upsert_workflow_information(
            identifier=identifier_one,
            workflow_information=workflow_one.for_storage(identifier_one, None),
        )
        assert store.retrieve_workflow_information(
            identifier=identifier_one
        ) == state.WorkflowInformation(
            workflow_code="test_workflow",
            workflow_version=1,
            information={"one": "one", "count": 3},
        )
        assert store.retrieve_workflow_information(
            identifier=identifier_two
        ) == state.WorkflowInformation(
            workflow_code="test_workflow",
            workflow_version=1,
            information={"one": "two", "count": 6},
        )

        store.upsert_workflow_information(
            identifier=identifier_two,
            workflow_information=workflow_two.for_storage(identifier_two, None),
        )
        assert store.retrieve_workflow_information(
            identifier=identifier_one
        ) == state.WorkflowInformation(
            workflow_code="test_workflow",
            workflow_version=1,
            information={"one": "one", "count": 3},
        )
        assert store.retrieve_workflow_information(
            identifier=identifier_two
        ) == state.WorkflowInformation(
            workflow_code="test_workflow",
            workflow_version=1,
            information={"one": "two", "count": 7},
        )

    def test_can_store_computations(self) -> None:
        store = M_Storage()

        with pytest.raises(errors.WorkflowNotFound):
            store.retrieve_computations(
                identifier=state.WorkflowIdentifier(identifier="__unknown__")
            )

        identifier = store.store_new_workflow(Workflow(one="one", count=1))
        assert store.retrieve_computations(identifier=identifier) == {}

        info1 = M_StoredInfo(state=M_State.fresh())
        info2 = M_StoredInfo(state=M_State.fresh())

        def P(path: str) -> protocols.Path:
            return tuple(path.split("."))

        store.upsert_computations(
            identifier=identifier, stored_infos={P("path.one"): info1, P("path.two"): info2}
        )

        assert store.retrieve_computations(identifier=identifier) == {
            P("path.one"): info1,
            P("path.two"): info2,
        }

        info1_updated = info1.merge(
            state.Results(original_state=info1.state).progressing(audit_message="progress")
        )
        info3 = M_StoredInfo(state=M_State.fresh())
        store.upsert_computations(
            identifier=identifier,
            stored_infos={P("path.one"): info1_updated, P("path.three"): info3},
        )

        assert store.retrieve_computations(identifier=identifier) == {
            P("path.one"): info1_updated,
            P("path.two"): info2,
            P("path.three"): info3,
        }

        info4 = M_StoredInfo(state=M_State.fresh())
        store.upsert_computations(
            identifier=identifier,
            stored_infos={
                P("path.one"): info1_updated,
                P("path.three"): info3,
                P("path.three.other"): info4,
            },
        )

        assert store.retrieve_computations(identifier=identifier) == {
            P("path.one"): info1_updated,
            P("path.two"): info2,
            P("path.three"): info3,
            P("path.three.other"): info4,
        }

    def test_it_can_hold_a_lock(self) -> None:
        called: list[tuple[str, str]] = []
        threads: list[threading.Thread] = []

        store = M_Storage()
        identifier = store.store_new_workflow(Workflow(one="one", count=1))

        def start_thread(desc: str, /) -> None:
            def hold_lock() -> None:
                called.append(("start", desc))
                with store.hold_workflow_lock(identifier):
                    called.append(("in", desc))
                called.append(("out", desc))

            thread = threading.Thread(target=hold_lock)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        start_thread("one")
        with store.hold_workflow_lock(identifier):
            called.append(("before", "two"))
            start_thread("two")
            called.append(("after_started", "two"))

        for thread in threads:
            thread.join()

        assert called == [
            ("start", "one"),
            ("in", "one"),
            ("out", "one"),
            ("before", "two"),
            ("start", "two"),
            ("after_started", "two"),
            ("in", "two"),
            ("out", "two"),
        ]
