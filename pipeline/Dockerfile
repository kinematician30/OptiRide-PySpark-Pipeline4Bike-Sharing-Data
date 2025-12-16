# 1. Start from a base image: using the spark image
FROM spark:python3-java17

# 2. permanent enviroment variable to add and execute spark
ENV PATH=$PATH:/opt/spark/bin

# 3. set the working directory to /app
WORKDIR /app

# 4. Switch to 'root' user for permission to instal; softwares
USER root

# 5. Copy the requirements.txt file
COPY requirements.txt .
# Run pip install to install the libraries
RUN pip install -r requirements.txt

# 6. Copy the entire content of your buildpoint (pipeline folder) into the /app working directory in the container.
COPY . .

# 7. make the container listen on the host os port
EXPOSE 4040

# 8. Switch back to the default user
USER spark