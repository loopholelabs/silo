// SPDX-License-Identifier: GPL-3.0

#ifndef SILO_LOG_H
#define SILO_LOG_H

#define _log_prepend_crit "[silo_pagefault (CRIT)]:"
#define _log_prepend_error "[silo_pagefault (ERROR)]:"
#define _log_prepend_warn "[silo_pagefault (WARN)]:"
#define _log_prepend_info "[silo_pagefault (INFO)]:"
#define _log_prepend_debug "[silo_pagefault (DEBUG)]:"
#define _log_prepend_trace "[silo_pagefault (TRACE)]:"
#define _log_prepend_follow "[silo_pagefault (FOLLOW)]:"

#define log_crit(fmt, ...)                                             \
	printk(KERN_CRIT _log_prepend_crit " " fmt "\n" __VA_OPT__(, ) \
		       __VA_ARGS__)
#define log_error(fmt, ...)                                            \
	printk(KERN_ERR _log_prepend_error " " fmt "\n" __VA_OPT__(, ) \
		       __VA_ARGS__)
#define log_warn(fmt, ...)                                                \
	printk(KERN_WARNING _log_prepend_warn " " fmt "\n" __VA_OPT__(, ) \
		       __VA_ARGS__)

#if LOG_LEVEL > 0
#define log_info(fmt, ...)                                             \
	printk(KERN_INFO _log_prepend_info " " fmt "\n" __VA_OPT__(, ) \
		       __VA_ARGS__)
#else
#define log_info(fmt, ...)
#endif

#if LOG_LEVEL > 1
#define log_debug(fmt, ...)                                              \
	printk(KERN_DEBUG _log_prepend_debug " " fmt "\n" __VA_OPT__(, ) \
		       __VA_ARGS__)
#else
#define log_debug(fmt, ...)
#endif

#if LOG_LEVEL > 2
#define log_trace(fmt, ...)                                              \
	printk(KERN_DEBUG _log_prepend_trace " " fmt "\n" __VA_OPT__(, ) \
		       __VA_ARGS__)
#else
#define log_trace(fmt, ...)
#endif

#if LOG_LEVEL > 3
#define log_follow(fmt, ...)                                              \
	printk(KERN_DEBUG _log_prepend_follow " " fmt "\n" __VA_OPT__(, ) \
		       __VA_ARGS__)
#else
#define log_follow(fmt, ...)
#endif

#define START_FOLLOW log_follow("start '%s'", __func__)
#define END_FOLLOW log_follow("end '%s'", __func__)

#endif //SILO_LOG_H
