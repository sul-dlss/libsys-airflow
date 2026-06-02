#!/bin/bash
set -e

echo "Replacing migration script with fixed version..."

python3 << 'PYTHON_EOF'
import sys
import os
import glob

pattern = '/home/airflow/.local/lib/python*/site-packages/airflow/migrations/versions/*remove_pickled_data_from_dagrun_table.py'
matches = glob.glob(pattern)

if not matches:
    print("Migration file not found!")
    sys.exit(0)

migration_path = matches[0]
print(f"Found migration at: {migration_path}")

# Check if already patched
with open(migration_path, 'r') as f:
    if 'bytes.fromhex' in f.read():
        print("Already patched!")
        sys.exit(0)

# Write the completely fixed migration
fixed_content = '''# Licensed to the Apache Software Foundation (ASF) under one
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

"""
remove pickled data from dagrun table.

Revision ID: e39a26ac59f6
Revises: 38770795785f
Create Date: 2025-01-23 12:00:00.000000

"""

from __future__ import annotations

import json
import pickle

from alembic import op
from sqlalchemy import text
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e39a26ac59f6"
down_revision = "38770795785f"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply remove pickled data from dagrun table."""
    conn = op.get_bind()
    
    # Add the conf_json column first
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("conf_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    
    if conn.dialect.name == "sqlite":
        empty_val = "X'80057D942E'"
    else:
        empty_val = r"'\\x80057D942E'"
    
    if conn.dialect.name == "mssql":
        conn.execute(
            text(
                "UPDATE dag_run "
                "SET conf_json = TRY_CAST(conf AS NVARCHAR(MAX)) "
                f"WHERE conf IS not NULL AND conf != {empty_val}"
            )
        )
    else:
        BATCH_SIZE = 1000
        offset = 0
        while True:
            err_count = 0
            batch_num = offset + 1
            print(f"converting dag run conf. batch={batch_num}")
            rows = conn.execute(
                text(
                    "SELECT id, conf "
                    "FROM dag_run "
                    "WHERE conf IS not NULL "
                    f"AND conf != {empty_val}"
                    f"ORDER BY id LIMIT {BATCH_SIZE} "
                    f"OFFSET {offset}"
                )
            ).fetchall()
            if not rows:
                break
            for row in rows:
                row_id, pickle_data = row

                # Use nested transaction (savepoint) for each row
                nested = conn.begin_nested()
                try:
                    # Handle hex-encoded bytea data
                    if isinstance(pickle_data, memoryview):
                        pickle_data = pickle_data.tobytes()
                    
                    # Try direct unpickling
                    try:
                        original_data = pickle.loads(pickle_data)
                    except (ValueError, pickle.UnpicklingError) as pickle_err:
                        # If it fails with protocol error, data might be hex-encoded
                        if "unsupported pickle protocol" in str(pickle_err):
                            try:
                                hex_str = pickle_data.decode('ascii')
                                actual_data = bytes.fromhex(hex_str)
                                original_data = pickle.loads(actual_data)
                            except Exception:
                                raise pickle_err
                        else:
                            raise
                    
                    json_data = json.dumps(original_data)
                    conn.execute(
                        text("""
                            UPDATE dag_run
                            SET conf_json = :json_data
                            WHERE id = :id
                        """),
                        {"json_data": json_data, "id": row_id},
                    )
                    nested.commit()  # Commit the savepoint
                except Exception as e:
                    nested.rollback()  # Rollback the savepoint
                    print(f"Error row {row_id}: {type(e).__name__}: {str(e)[:80]}")
                    err_count += 1
                    continue
            if err_count:
                print(f"could not convert dag run conf for {err_count} records. batch={batch_num}")
            offset += BATCH_SIZE
    
    # Drop the old conf column and rename conf_json to conf
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("conf")
        batch_op.alter_column("conf_json", new_column_name="conf")


def downgrade():
    """Unapply remove pickled data from dagrun table."""
    pass
'''

with open(migration_path, 'w') as f:
    f.write(fixed_content)

print(f"✓ Completely replaced migration at {migration_path}")
PYTHON_EOF

echo "Continuing with original entrypoint..."
exec /entrypoint "$@"