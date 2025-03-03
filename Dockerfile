FROM python:3.9 

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies with pip upgrade
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Add environment variables for PayPal credentials
ENV PAYPAL_CLIENT_ID=""
ENV PAYPAL_CLIENT_SECRET=""
ENV PAYPAL_SANDBOX=True

CMD ["python", "src/main.py"]