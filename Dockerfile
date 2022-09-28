FROM python:3.9-alpine
WORKDIR /arbiter
ENV PYTHONUNBUFFERED=1
EXPOSE 5000 9443 80 443
COPY /bot /arbiter
COPY /tests /arbiter
COPY requirements.txt /arbiter
RUN python -m pip install --upgrade pip && pip install -r requirements.txt
CMD ["python", "bot.py"]