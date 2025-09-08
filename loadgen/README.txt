build
mvn install -D skipTest


run
java -jar target/loadgen-1.0-SNAPSHOT.jar local.yaml

stop:
Ctrl-C

To run loadgen in Docker container:
docker build -t loadgen:1.0 . - build an image
docker run -d --rm -v /path/to/config_folder:/usr/bin/loadgen/config java-loadgen:1.0 /usr/bin/loadgen/config/loadtest.yaml - run image in a detached mode, mount config folder

Run loadgen in k8s:

apiVersion: v1
kind: ConfigMap
metadata:
  name: loadgen-config
data:
  loadtest.yaml: |
    # config goes here

apiVersion: v1
kind: Pod
metadata:
  name: loadgen
spec:
  containers:
  - name: loadgen
    image: loadgen:1.0
    args: ["/usr/bin/loadgen/config/loadtest.yaml"]
    volumeMounts:
    - name: config-volume
      mountPath: /usr/bin/loadgen/config
  volumes:
  - name: config-volume
    configMap:
      name: loadgen-config