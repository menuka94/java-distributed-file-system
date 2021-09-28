
JAR_FILE=../../../libs/cs555-hw1-1.0.jar
CLASSES_DIR=build/classes/java/main

.PHONY: build
build:
	chmod +x ./gradlew
	./gradlew build

controller:
	# On arkansas.cs.colostate.edu
	#./delete-chunks.sh
	cd $(CLASSES_DIR) && java -cp $(JAR_FILE) cs555.hw1.node.Controller 9000

controller-debug:
	# On arkansas.cs.colostate.edu
	cd $(CLASSES_DIR) && java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -cp $(JAR_FILE) cs555.hw1.node.Controller 9000

cs:
	./start-chunk-server.sh

chunk-server:
	./start-chunk-server.sh

chunk-server-debug:
	cd $(CLASSES_DIR) && java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005  -cp $(JAR_FILE) cs555.hw1.node.chunkServer.ChunkServer arkansas 9000

client:
	cd $(CLASSES_DIR) && java  -cp $(JAR_FILE) cs555.hw1.node.Client arkansas 9000

client-debug:
	cd $(CLASSES_DIR) && java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -cp $(JAR_FILE) cs555.hw1.node.Client arkansas 9000

clean:
	rm -rf build
