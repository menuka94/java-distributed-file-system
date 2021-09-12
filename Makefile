.PHONY: build
build:
	chmod +x ./gradlew
	./gradlew build

controller:
	./start-controller.sh

chunk-server:
	./start-chunk-server.sh

client:
	./start-client.sh

clean:
	rm -rf build

start:
	cd build/classes/java/main && java -cp ../../../libs/cs555-hw1-1.0.jar cs555.hw1.Main