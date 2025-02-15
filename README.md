# Session Manager dengan Redis Lock

Implementasi manajemen session menggunakan Redis untuk distributed locking dengan dua pendekatan berbeda.

## Perbandingan Implementasi Session Management

### 1. ProcessWithSession (Hash-based)
#### Karakteristik
- Menggunakan Redis Hash untuk menyimpan session
- Menggunakan Watch/Transaction untuk atomic operation
- Status session disimpan dalam hash (`in_use`/`available`)
- Membutuhkan cleanup untuk session yang expired

#### Flow
1. Watch key "sessions"
2. Get semua session dari hash
3. Cek session available atau buat baru
4. Set status "in_use" dalam transaction
5. Lock session dengan SetNX
6. Release dengan mengubah status jadi "available"

#### Pros
- Status session terpusat dalam satu hash
- Mudah untuk monitoring semua session
- Transaction memastikan konsistensi data
- Performa lebih cepat (6x lebih cepat dari List)
- Waktu eksekusi konsisten (~5 detik)

#### Cons
- Memory usage tinggi (~80-85KB per operasi)
- Jumlah alokasi memory tinggi (~940 alokasi)
- Overhead dari Watch/Transaction
- Kompleksitas lebih tinggi

### 2. NewProcessWithSession (List-based)
#### Karakteristik
- Menggunakan Redis List sebagai session pool
- Operasi atomic dengan RPOP/LPUSH
- Tidak menyimpan status session
- Self-cleaning karena session yang tidak di pool berarti sedang digunakan

#### Flow
1. RPOP dari session pool
2. Jika pool kosong, buat session baru
3. Lock session dengan SetNX
4. Release dengan LPUSH kembali ke pool

#### Pros
- Memory usage sangat efisien (~10KB per operasi)
- Alokasi memory minimal (~270 alokasi)
- Tidak perlu mekanisme cleanup khusus
- Atomic operations tanpa transaction
- Memory usage lebih stabil di berbagai level concurrent

#### Cons
- Waktu eksekusi lebih lambat (~30 detik)
- Tidak ada visibility status session
- Monitoring lebih sulit
- Latency tinggi karena RPOP/LPUSH berulang

## Performa (Berdasarkan Benchmark)

### Hash-based (5 Concurrent Workers)
- Waktu: ~5 detik
- Memory: 84.7 KB
- Alokasi: 944 kali
- Performa konsisten di berbagai level concurrent

### List-based (5 Concurrent Workers)
- Waktu: ~30 detik
- Memory: 10.7 KB
- Alokasi: 268 kali
- Memory usage lebih efisien pada concurrent rendah

## Use Cases

### Hash-based cocok untuk:
- Real-time applications
- High-throughput systems
- Sistem yang membutuhkan monitoring detail
- Aplikasi yang sensitif terhadap latency
- Sistem dengan memory yang cukup

### List-based cocok untuk:
- Background jobs
- Resource-constrained systems
- Long-running processes
- Sistem dengan memory terbatas
- Aplikasi yang tidak sensitif terhadap latency

## Mekanisme Lock
- Format: `lock:{sessionID}`
- TTL: 5 menit
- Command: `SET lock:{sessionID} {userID} EX 300 NX`

## Format Session ID
- Pattern: `session-{randomstring}`
- Random string: 12 karakter
- Charset: a-zA-Z0-9

## Cara Menjalankan

```bash
# Run aplikasi
go run main.go

# Run benchmark dengan memory allocation
go test -bench=. -benchmem

```

## Detail Implementasi

### Case-Case yang Ditangani

#### Case 1: First Session (Session Pertama)
##### Hash-based:
- Kondisi: Hash "sessions" kosong
- Aksi: Buat session baru dengan status "in_use"
- Command: `HSET sessions {session-id} "in_use"`

##### List-based:
- Kondisi: List "session_pool" kosong (RPOP returns nil)
- Aksi: Buat session baru
- Command: `RPOP session_pool` -> nil -> generate new session

#### Case 2: All Sessions In Use
##### Hash-based:
- Kondisi: Semua session dalam hash berstatus "in_use"
- Aksi: Buat session baru dengan status "in_use"
- Command: `HSET sessions {session-id} "in_use"`

##### List-based:
- Kondisi: List "session_pool" kosong (semua session sedang digunakan)
- Aksi: Buat session baru
- Command: `RPOP session_pool` -> nil -> generate new session

#### Case 3: Available Session
##### Hash-based:
- Kondisi: Ada session dengan status "available"
- Aksi: Update status session menjadi "in_use"
- Command: `HSET sessions {session-id} "in_use"`

##### List-based:
- Kondisi: Ada session dalam pool
- Aksi: Ambil session yang tersedia
- Command: `RPOP session_pool` -> return existing session

#### Release Session
##### Hash-based:
- Aksi: Set status session menjadi available
- Command: `HSET sessions {session-id} "available"`

##### List-based:
- Aksi: Kembalikan session ke pool
- Command: `LPUSH session_pool {session-id}`