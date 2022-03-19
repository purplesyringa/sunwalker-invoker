use anyhow::{Result, bail};
use libc::{c_void, c_int, pid_t, clone};


extern fn fn_runner<F: std::ops::Fn() -> ()>(ptr: *mut c_void) -> i32 {
    unsafe {
        (&mut *(ptr as *mut F))();
    }
    0
}


pub fn spawn<F: std::ops::Fn() -> ()>(mut f: F) -> Result<pid_t> {
    let stack: &mut [u8] = &mut [0u8; 4096];  // 4 KiB ought to be enough for everyone
    let flags: c_int = 0; // todo: CLONE_VFORK
    let pid = unsafe {
        clone(fn_runner::<F>, stack.as_mut_ptr_range().end as *mut c_void, flags, &mut f as *mut F as *mut c_void)
    };
    if pid == -1 {
        bail!(std::io::Error::last_os_error())
    } else {
        Ok(pid)
    }
}


pub fn getpid() -> pid_t {
    unsafe { libc::getpid() }
}
