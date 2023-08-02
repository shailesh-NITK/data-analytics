FROM quay.io/astronomer/astro-runtime:8.8.0
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64
RUN export JAVA_HOME
#RUN pip install --upgrade pip
RUN python3 -m pip install matplotlib
RUN pip install pandas
## DBT 
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
 pip install --no-cache-dir -r dbt-requirements.txt && deactivate