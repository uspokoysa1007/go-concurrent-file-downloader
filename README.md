# go-concurrent-file-downloader

Высокоскоростной многопоточный загрузчик файлов на Go с использованием HTTP Range и горутин (`goroutines`).

## Возможности
- Определение поддержки сервером докачки и многопоточности (`Accept-Ranges: bytes`).
- Разделение большого файла на N равных частей (чанков) с параллельной загрузкой.
- Индивидуальный прогресс-бар для каждого потока в реальном времени.
- Бесшовное объединение скачанных фрагментов в итоговый файл.
- Встроенный демонстрационный режим (`-demo`) с локальным тестовым HTTP-сервером (для проверки без внешнего интернета).

## Сборка
```bash
go build -o releases/go-concurrent-file-downloader.exe main.go
```

## Использование
```bash
# Запуск демонстрационного теста
./releases/go-concurrent-file-downloader.exe -demo

# Скачивание файла в 4 параллельных потока
./releases/go-concurrent-file-downloader.exe -url https://example.com/largefile.iso -out largefile.iso -concurrency 4
```
