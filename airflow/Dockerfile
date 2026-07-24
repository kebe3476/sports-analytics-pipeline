FROM apache/airflow:2.10.0
COPY requirements.txt /requirements.txt
# nfl-data-py pins pandas<2.0 but Airflow ships with pandas 2.x.
# Install it without deps so pip does not try to downgrade pandas,
# then add the two deps it needs that are not already in the image.
RUN pip install --no-cache-dir --no-deps "nfl-data-py>=0.3.0" \
    && pip install --no-cache-dir "pyarrow>=6.0" tqdm appdirs \
    && pip install --no-cache-dir -r /requirements.txt
