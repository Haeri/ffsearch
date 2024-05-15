# Stage 1: Build C++ executable
FROM gcc:latest AS builder
WORKDIR /app

# Copy the C++ source code
COPY ffsearch.cpp .
COPY unidecode.h .

# Compile the C++ code
RUN g++ -std=c++17 ffsearch.cpp -o ffsearch -O3 -static



# Stage 2: Final image with PHP and compiled binary
FROM php:latest
WORKDIR /var/www/html

# Copy the PHP file into the container
COPY index.php .
COPY --from=builder /app/ffsearch /var/www/html/

# Expose port 80 to the outside world
EXPOSE 80

# Start PHP server to run the script
CMD ["php", "-S", "0.0.0.0:80", "-t", "/var/www/html/"]
