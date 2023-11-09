build:
	go build -o microfts

mkindex:
	rm index
	./microfts create index
	find -type f -name \*.go |xargs ./microfts input index

test: build
	./microfts info -grams index  >debug.txt
