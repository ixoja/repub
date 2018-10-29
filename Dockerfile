FROM scratch
ADD main /
ADD html /html
ENV HTML="/html"
CMD ["/main"]