

class FedCallHolder:
    """
    `FedCallHolder` represents a call node holder when submitting tasks.
    For example,

    f.party("ALICE").remote()
    ~~~~~~~~~~~~~~~~
        ^
        |
    it's a holder.

    """
    def __init__(
        self,
        node_party,
        submit_ray_task_func,
        options={},
    ) -> None:
        # Note(NKcqx): FedCallHolder will only be created in driver process, where
        # the GlobalContext must has been initialized.
        # job_name = get_global_context().get_job_name()
        # self._party = fed_config.get_cluster_config(job_name).current_party
        self._node_party = node_party
        self._options = options
        self._submit_ray_task_func = submit_ray_task_func
        