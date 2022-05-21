#!/bin/sh

echo '--- Start Extract CPU ---'
bash bin/extract_cpu.sh

echo '--- Start Extract jbb2015 ---'
bash bin/extract_jbb2015.sh

echo '--- Start Extract jvm2008 ---'
bash bin/extract_jvm2008.sh

echo '--- Start Extract ssj2008 ---'
bash bin/extract_ssj2008.sh
