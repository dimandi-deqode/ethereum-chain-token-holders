FROM python:3.10
WORKDIR /app
COPY . .
RUN pip3 install -r requirements.txt
CMD ["bash", "-c", "python3 /app/app.py && tail -f /dev/null"]
