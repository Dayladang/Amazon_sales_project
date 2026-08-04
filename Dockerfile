# FROM python:3.12.3-slim
# # Cài đặt uv từ image chính thức của Astral
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# WORKDIR /app
# # Thêm uv vào PATH nên có thể sử dụng uv trực tiếp mà không cần chỉ định đường dẫn đầy đủ
# ENV PATH="/app/.venv/bin:$PATH"

# COPY pyproject.toml uv.lock .python-version ./
# # Cài đặt dependencies bằng uv, sử dụng lockfile để đảm bảo tính nhất quán
# RUN uv sync --locked

# COPY dataIngestion.py dataIngestion.py

# ENTRYPOINT ["python", "dataIngestion.py"]

FROM apache/airflow:3.2.1

USER root

RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow 

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt