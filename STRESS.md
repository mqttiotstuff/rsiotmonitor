
# Measures

using the tokio / tower dependencies (full), the release binary (linked), enlarge from 2mb
passing from 3,7M to 5,7M

-rwxrwxr-x   2 use use 5,7M févr.  5 13:32 rsiotmonitor*
-rw-rw-r--   1 use use 1,7K févr.  5 13:32 rsiotmonitor.d
use@alexa:~/rsiotmonitor/target/release$ ldd rsiotmonitor
	linux-vdso.so.1 (0x00007fff4dee0000)
	libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x00007fbd5b255000)
	libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fbd5b16e000)
	libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fbd5af46000)
	/lib64/ld-linux-x86-64.so.2 (0x00007fbd5b859000)



## using hyper, and http service the following service gives this result :


100 concurrent client, and 9000 requests, with a no op request handler


use@alexa:~/Téléchargements$ h2load -n9000 -c100 http://localhost:3000
starting benchmark...
spawning thread #0: 100 total client(s). 9000 total requests
Application protocol: h2c
progress: 10% done
progress: 20% done
progress: 30% done
progress: 40% done
progress: 50% done
progress: 60% done
progress: 70% done
progress: 80% done
progress: 90% done
progress: 100% done

finished in 1.04s, 8646.16 req/s, 345.94KB/s
requests: 9000 total, 9000 started, 9000 done, 9000 succeeded, 0 failed, 0 errored, 0 timeout
status codes: 9000 2xx, 0 3xx, 0 4xx, 0 5xx
traffic: 360.10KB (368741) total, 21.33KB (21841) headers (space savings 94.36%), 96.68KB (99000) data
                     min         max         mean         sd        +/- sd
time for request:      424us     75.90ms     11.11ms      6.27ms    84.36%
time for connect:     1.68ms      4.63ms      3.17ms       737us    63.00%
time to 1st byte:     9.71ms     80.84ms     51.85ms     22.94ms    59.00%
req/s           :      86.57       99.74       89.70        3.07    82.00%


