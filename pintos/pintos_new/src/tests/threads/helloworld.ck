# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF']);
(helloworld) begin
Hello World!
(helloworld) end
EOF
pass;
