from crontab import CronTab

cron = CronTab(user=True)

max_age_days = 365


# Runs before the file removals below: this drops the task_instance rows and the
# 00:40 job deletes the log files they point at. The shell computes the timestamp
# at run time so the cutoff keeps moving. --skip-archive hard-deletes; without it
# Airflow copies every row into _airflow_deleted__* tables and nothing is reclaimed.
# Airflow logs to stdout, so discarding it drops both the per-table narration and the
# warnings about tables this version no longer has; stderr still reaches cron's mail.
db_clean = cron.new(
    command=(
        'docker exec libsys_airflow-airflow-scheduler-1 '
        'airflow db clean --clean-before-timestamp '
        f'"$(date -d \'{max_age_days} days ago\' --iso-8601=date)T00:00:00" '
        '--yes --skip-archive > /dev/null'
    )
)
db_clean.dow.on('SUN')
db_clean.hour.on(0)
db_clean.minute.on(0)

data_export_files = cron.new(
    command=f'find /home/libsys/libsys-airflow/shared/data-export-files -type f -mtime +{max_age_days} -delete'
)
data_export_files.dow.on('SUN')
data_export_files.hour.on(0)
data_export_files.minute.on(10)

data_export_dirs = cron.new(
    command='find /home/libsys/libsys-airflow/shared/data-export-files -type d -empty -delete'
)
data_export_dirs.dow.on('SUN')
data_export_dirs.hour.on(0)
data_export_dirs.minute.on(15)

digital_bookplates_files = cron.new(
    command=f'find /home/libsys/libsys-airflow/shared/digital-bookplates -type f -mtime +{max_age_days} -delete'
)
digital_bookplates_files.dow.on('SUN')
digital_bookplates_files.hour.on(0)
digital_bookplates_files.minute.on(20)

digital_bookplates_dirs = cron.new(
    command='find /home/libsys/libsys-airflow/shared/digital-bookplates -type d -empty -delete'
)
digital_bookplates_dirs.dow.on('SUN')
digital_bookplates_dirs.hour.on(0)
digital_bookplates_dirs.minute.on(25)

fix_encumbrances_files = cron.new(
    command=f'find /home/libsys/libsys-airflow/shared/fix_encumbrances -type f -mtime +{max_age_days} -delete'
)
fix_encumbrances_files.dow.on('SUN')
fix_encumbrances_files.hour.on(0)
fix_encumbrances_files.minute.on(30)

fix_encumbrances_dirs = cron.new(
    command='find /home/libsys/libsys-airflow/shared/fix_encumbrances -type d -empty -delete'
)
fix_encumbrances_dirs.dow.on('SUN')
fix_encumbrances_dirs.hour.on(0)
fix_encumbrances_dirs.minute.on(35)

logs_files = cron.new(
    command=f'find /home/libsys/libsys-airflow/shared/logs -type f -mtime +{max_age_days} -delete'
)
logs_files.dow.on('SUN')
logs_files.hour.on(0)
logs_files.minute.on(40)

authority_files = cron.new(
    command=f"find /home/libsys/libsys-airflow/shared/authorities -type f -mtime +{max_age_days} -delete"
)
authority_files.dow.on('SUN')
authority_files.hour.on(0)
authority_files.minute.on(45)

authority_dirs = cron.new(
    command="find /home/libsys/libsys-airflow/shared/authorities -type d -empty -delete"
)
authority_dirs.dow.on('SUN')
authority_dirs.hour.on(0)
authority_dirs.minute.on(50)

sdr_files = cron.new(
    command=f"find /home/libsys/libsys-airflow/shared/sdr-files -type f -mtime +{max_age_days} -delete"
)
sdr_files.dow.on('SUN')
sdr_files.hour.on(0)
sdr_files.minute.on(55)

sdr_dirs = cron.new(
    command="find /home/libsys/libsys-airflow/shared/sdr-files -type d -empty -delete"
)
sdr_dirs.dow.on('SUN')
sdr_dirs.hour.on(1)
sdr_dirs.minute.on(0)

cron.write()
