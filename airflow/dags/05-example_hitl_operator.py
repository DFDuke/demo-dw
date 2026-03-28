# https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html#

class LocalLogNotifier(BaseNotifier):
    """Simple notifier to demonstrate HITL notification without setup any connection."""

    template_fields = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message

    def notify(self, context: Context) -> None:
        url = HITLOperator.generate_link_to_ui_from_context(
            context=context,
            base_url="http://localhost:28080",
        )
        self.log.info(self.message)
        self.log.info("Url to respond %s", url)


hitl_request_callback = LocalLogNotifier(
    message="""
[HITL]
Subject: {{ task.subject }}
Body: {{ task.body }}
Options: {{ task.options }}
Is Multiple Option: {{ task.multiple }}
Default Options: {{ task.defaults }}
Params: {{ task.params }}
"""
)
hitl_success_callback = LocalLogNotifier(
    message="{% set task_id = task.task_id -%}{{ ti.xcom_pull(task_ids=task_id) }}"
)
hitl_failure_callback = LocalLogNotifier(message="Request to response to '{{ task.subject }}' failed")

with DAG(
    dag_id="example_hitl_operator",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "HITL"],
):
    wait_for_input = HITLEntryOperator(
        task_id="wait_for_input",
        subject="Please provide required information: ",
        params={"information": Param("", type="string")},
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    wait_for_option = HITLOperator(
        task_id="wait_for_option",
        subject="Please choose one option to proceed: ",
        options=["option 1", "option 2", "option 3"],
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    wait_for_multiple_options = HITLOperator(
        task_id="wait_for_multiple_options",
        subject="Please choose option to proceed: ",
        options=["option 4", "option 5", "option 6"],
        multiple=True,
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    wait_for_default_option = HITLOperator(
        task_id="wait_for_default_option",
        subject="Please choose option to proceed: ",
        options=["option 7", "option 8", "option 9"],
        defaults=["option 7"],
        execution_timeout=datetime.timedelta(seconds=1),
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    valid_input_and_options = ApprovalOperator(
        task_id="valid_input_and_options",
        subject="Are the following input and options valid?",
        body="""
        Input: {{ ti.xcom_pull(task_ids='wait_for_input')["params_input"]["information"] }}
        Option: {{ ti.xcom_pull(task_ids='wait_for_option')["chosen_options"] }}
        Multiple Options: {{ ti.xcom_pull(task_ids='wait_for_multiple_options')["chosen_options"] }}
        Timeout Option: {{ ti.xcom_pull(task_ids='wait_for_default_option')["chosen_options"] }}
        """,
        defaults="Reject",
        execution_timeout=datetime.timedelta(minutes=1),
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
        assigned_users=[{"id": "admin", "name": "admin"}],
    )
    choose_a_branch_to_run = HITLBranchOperator(
        task_id="choose_a_branch_to_run",
        subject="You're now allowed to proceeded. Please choose one task to run: ",
        options=["task_1", "task_2", "task_3"],
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    @task
    def task_1(): ...

    @task
    def task_2(): ...

    @task
    def task_3(): ...

    (
        [wait_for_input, wait_for_option, wait_for_default_option, wait_for_multiple_options]
        >> valid_input_and_options
        >> choose_a_branch_to_run
        >> [task_1(), task_2(), task_3()]
    )
