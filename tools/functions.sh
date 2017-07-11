# get the number of physical cores
echo_num_cores () {
	local ncpu=

	case "$OSTYPE" in
		darwin*)
			ncpu=$(sysctl -n hw.physicalcpu)
			;;
		linux*)
			ncpu=$(cat /proc/cpuinfo | grep 'core id' | sort -u | wc -l)
			;;
		*)
			ncpu=2
			;;
	esac
	echo $ncpu
}
