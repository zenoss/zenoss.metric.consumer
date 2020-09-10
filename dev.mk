Dockerfile: Dockerfile.in
	@sed -e "s/%UID%/$$(id -u)/g" -e "s/%GID%/$$(id -g)/g" < $< > $@

.PHONY: image
image: Dockerfile
	@docker build -t zenoss/java11 .

.PHONY: shell
shell: Dockerfile
	@docker run -it --rm -v $(PWD):/mnt/src -v $(HOME):/home/build zenoss/java11 /bin/bash
