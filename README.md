# kinesis-example

### Step 1 
Clone the project, navigate to the root path of the project and run command `gradle eclipse`.

### Step 2
In 'application.properties' file inside `java/main/resouces` fill in the awssecret,awskey, stream name and application name for the stream.

### Step 3
Run the main class to start tomcat container.

### 
Send POST request to url `localhost:8080/persons/publish` with json data `{"name" : "John Doe"}` to publish data to stream.

