# Airflow Install For EC2

# Install

## User & Group Add

```bash
groupadd airflow
useradd airflow -g airflow -d /airflow -m

su - airflow
cat <<EOF >> ~/.bashrc
export AIRFLOW_HOME=/var/lib/airflow
EOF
```

## Install Developements Package

```bash
yum install python3 python3-devel wget gcc gcc-c++ make

wget https://www.sqlite.org/2021/sqlite-autoconf-3340100.tar.gz
tar xvf sqlite-autoconf-3340100.tar.gz
cd sqlite-autoconf-3340100
./configure
make
make install
ls /usr/local/lib

vi /etc/environment # or /etc/profile
# 에 아래 줄 추가
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
source /etc/environment
```

## Install Airflow

```bash
python3 -m pip install -U pip
python3 -m pip install apache-airflow==2.1.4 \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.1.4/constraints-3.6.txt"

airflow db init
```

## Install Postgresql

```bash
yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
yum install -y postgresql11-server postgresql11-contrib
/usr/pgsql-11/bin/postgresql-11-setup initdb
systemctl enable postgresql-11
systemctl start pstgresql-11

python3 -m pip install psycopg2 psycopg2-binary
```

## Airflow Default DB Change For postgresql

```bash
su postgres
psql

postgres=# CREATE USER airflow PASSWORD 'airflow';
CREATE ROLE
postgres=# CREATE DATABASE airflow;
CREATE DATABASE
```

## Airflow DB 및 일부 설정 파라미터 변경

```bash
$AIRFLOW_HOME/airflow.cfg 파일 수정

수정 내역:
1. default_timezone = Asia/Seoul
2. executor = LocalExecutor
3. sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
4. load_examples = False
```

## Airflow DB init

```bash
su airflow

airflow db init
```

## Airflow Daemon 등록

```bash
cat <<EOF > /usr/lib/tmpfiles.d/airflow.conf 
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

D /run/airflow 0755 airflow airflow
EOF

cat <<EOF >/usr/lib/systemd/system/airflow-webserver.service
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
Environment=AIRFLOW_HOME=/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow webserver -p 8080
Restart=on-failure
RestartSec=10s
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

cat <<EOF > /usr/lib/systemd/system/airflow-scheduler.service
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
Environment=AIRFLOW_HOME=/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow scheduler
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
EOF
```

airflow daemon start

```bash
# Airflow 관련 추가한 데몬 파일 load
systemctl daemon-reload

systemctl enable airflow-webserver
systemctl enable airflow-scheduler

systemctl start airflow-webserver
systemctl start airflow-scheduler
```

# Mount

Disk Mount 여부 확인

```bash
lsblk
```

![Untitled](img/Untitled.png)

Disk 초기화 진행

```bash
# 디스크 초기화 진행(windows 포맷과 유사)
fdisk /dev/xvdb
# 파일 타입 xfs 로 변경
mkfx.xfs /dev/xvdb1

# disk UUID 확인
blkid
```

![Untitled](img/Untitled%201.png)

```bash
# 영구 마운트 진행 
cat <<EOF >> /etc/fstab
UUID=9b3bcefb-46b7-490a-9e3c-42bc54b8b73d /airflow		  xfs	  defaults	  0 0
EOF
# 마운트
mount -a

# 마운트 여부 확인
df -h -T
```

## Airflow DB 및 일부 설정 파라미터 변경

```bash
$AIRFLOW_HOME/airflow.cfg 파일 수정

수정 내역:
1. default_timezone = Asia/Seoul
2. executor = LocalExecutor
3. sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
4. load_examples = False
```