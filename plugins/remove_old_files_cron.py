from crontab import CronTab

cron = CronTab(user=True)

data_export_files = cron.new(
    command='find /home/libsys/libsys-airflow/shared/data-export-files -type f -mtime +10 -delete'
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
    command='find /home/libsys/libsys-airflow/shared/digital-bookplates -type f -mtime +10 -delete'
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
    command='find /home/libsys/libsys-airflow/shared/fix-encumbrances -type f -mtime +10 -delete'
)
fix_encumbrances_files.dow.on('SUN')
fix_encumbrances_files.hour.on(0)
fix_encumbrances_files.minute.on(30)

fix_encumbrances_dirs = cron.new(
    command='find /home/libsys/libsys-airflow/shared/fix-encumbrances -type d -empty -delete'
)
fix_encumbrances_dirs.dow.on('SUN')
fix_encumbrances_dirs.hour.on(0)
fix_encumbrances_dirs.minute.on(35)

logs_files = cron.new(
    command='find /home/libsys/libsys-airflow/shared/logs -type f -mtime +10 -delete'
)
logs_files.dow.on('SUN')
logs_files.hour.on(0)
logs_files.minute.on(40)

logs_dirs = cron.new(
    command='find /home/libsys/libsys-airflow/shared/logs -type d -empty -delete'
)
logs_dirs.dow.on('SUN')
logs_dirs.hour.on(0)
logs_dirs.minute.on(45)

cron.write()