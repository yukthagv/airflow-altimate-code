# Airflow 3 Migration: upgraded from astro-runtime:8.6.0 (Airflow 2.6.3, Python 3.9)
# astro-runtime:3.2 is the Astronomer-native Airflow 3 runtime (Airflow 3.2.x, Python 3.13)
# Astronomer runtime versioning changed: Airflow 3-era runtimes use airflow-version tags (e.g. 3.2)
# rather than the old sequential numbering (e.g. 8.x, 10.x, 12.x which are Airflow 2 runtimes)
FROM --platform=linux/amd64 quay.io/astronomer/astro-runtime:3.2