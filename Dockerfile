FROM public.ecr.aws/lambda/python:3.8

# Install dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy source code
COPY lambda_function.py youtube_video.py utils.py ${LAMBDA_TASK_ROOT}

# Set the command to be run when the container starts
CMD ["lambda_function.lambda_handler"]
