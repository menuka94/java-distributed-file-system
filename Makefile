.PHONY: build
build:
	./gradlew build

clean:
	rm -rf build

start:
	cd build/classes/java/main && java -cp ../../../libs/cs555-hw1-1.0.jar cs555.hw1.Main