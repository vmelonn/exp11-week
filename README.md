# exp11 seven-day rounds, cloud runner

Exported from `research-pipeline/experiment_11/11.1_data_plane/2_week/` by `export_cloud_runner.py`. Edit there, not here.

The workflow runs `run_week.py round` hourly. Atlas holds the state, so this runner and the laptop's scheduled task can both be active; see `run_week.py`.

Needs the repository secret `RIPE_API_KEY`. Results stay on RIPE Atlas; fetch them on the laptop with `run_week.py fetch`.
