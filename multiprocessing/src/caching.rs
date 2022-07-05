use crate::imp;
use itertools::Itertools;
use lazy_static::lazy_static;
use nix::{
    libc::{c_void, off_t},
    sys::mman,
};
use std::alloc::Allocator;
use std::arch::asm;
use std::cell::{SyncUnsafeCell, UnsafeCell};
use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::os::unix::io::RawFd;

#[derive(Eq, PartialEq, Debug)]
struct VMMap {
    range_start: *const u8,
    range_end: *const u8,
    prot: mman::ProtFlags,
    shared: bool,
    offset: off_t,
    dev_major: u8,
    dev_minor: u8,
    inode: u64,
    file: String,
    fd: RawFd,
    cache_base: *mut u8,
}

unsafe impl Sync for VMMap {}
unsafe impl Send for VMMap {}

// This data is preserved across restoration
struct TransparentData {
    stack_bottom: *mut u8,
    stack_size: usize,
    entry_rx_fd: RawFd,
}

struct CachedData {
    maps: Vec<VMMap>,
    rsp: *const u8,
    rip: *const u8,
    cache_mappings: Vec<(*const u8, *const u8)>,
    tls_base: usize,
    brk: *const c_void,
    retained_fds: HashSet<RawFd>,
    transparent_data: Option<&'static UnsafeCell<TransparentData>>,
}

unsafe impl Sync for CachedData {}
unsafe impl Send for CachedData {}

lazy_static! {
    // Handling the lock correctly over longjmp/setjmp is unfortunately too complicated
    static ref CACHED_DATA: SyncUnsafeCell<CachedData> = SyncUnsafeCell::new(CachedData {
        maps: Vec::new(),
        rsp: std::ptr::null(),
        rip: std::ptr::null(),
        cache_mappings: Vec::new(),
        tls_base: 0,
        brk: std::ptr::null(),
        retained_fds: HashSet::new(),
        transparent_data: None,
    });
}

pub(crate) fn cache_current_state() {
    // This function creates a sort of a snapshot of memory which can be later rolled back to.
    // Unfortunately this is not as simple as memcpy'ing the appropriate data somewhere and then
    // restoring it later, because during this process the program would use heap and stack, modify
    // glibc data, and so on.
    //
    // So we split this function into two stages. Firstly, we determine what we want to cache, while
    // being careful not to change the *boundaries* of the maps. Secondly, we perform the actual
    // caching while preserving the *contents* of the cached maps, except for the region of stack
    // that the caching subroutine uses. This detail is not important because said subroutine will
    // not access any of its variables upon restoration from cache.
    //
    // This necessitates a dedicated working area for the caching process. We will not use this area
    // from day one, but use the heap and the rest as usual and then move the data to the working
    // area just before starting to copy stuff around. This working area would not be cached.

    probe();

    let cached_data = unsafe { &mut *CACHED_DATA.get() };

    // Load current memory maps
    cached_data.maps.extend(get_current_maps(true));

    let mut cache_mappings: Vec<(*const u8, *const u8)> = Vec::new();

    unsafe {
        let size = (std::mem::size_of::<TransparentData>() + 4095) & !4095;

        let base = mman::mmap(
            std::ptr::null_mut(),
            size,
            mman::ProtFlags::PROT_READ | mman::ProtFlags::PROT_WRITE,
            mman::MapFlags::MAP_PRIVATE | mman::MapFlags::MAP_ANONYMOUS,
            -1,
            0,
        )
        .expect("Failed to mmap transparent data") as *const u8;
        cache_mappings.push((base, base.add(size)));
        cached_data.transparent_data = Some(&*(base as *const UnsafeCell<TransparentData>));
    }

    // We assume that read-only maps are not going to be modified from now on, which is perhaps a
    // reasonable compromise between caching everything and nothing
    for map in cached_data.maps.iter_mut() {
        if map.fd != -1 {
            cached_data.retained_fds.insert(map.fd);
        }

        if map.prot & (mman::ProtFlags::PROT_READ | mman::ProtFlags::PROT_WRITE)
            == (mman::ProtFlags::PROT_READ | mman::ProtFlags::PROT_WRITE)
            && !map.shared
        {
            let size = map.range_end as usize - map.range_start as usize;

            // Cache the whole range in a new page range
            unsafe {
                map.cache_base = mman::mmap(
                    std::ptr::null_mut(),
                    size,
                    mman::ProtFlags::PROT_READ | mman::ProtFlags::PROT_WRITE,
                    mman::MapFlags::MAP_PRIVATE | mman::MapFlags::MAP_ANONYMOUS,
                    -1,
                    0,
                )
                .expect("Failed to mmap a range") as *mut u8;

                cache_mappings.push((
                    map.cache_base as *const u8,
                    (map.cache_base as *const u8).add(size),
                ));
            }

            // Don't copy anything yet, because the data we ought to copy may be modified in the
            // process (no pun intended), e.g. if it's on stack
        }
    }

    cache_mappings.sort();
    cached_data.cache_mappings.extend(cache_mappings);

    // Save TLS
    if unsafe {
        nix::libc::syscall(
            nix::libc::SYS_arch_prctl,
            0x1003, // ARCH_GET_FS
            &mut cached_data.tls_base,
        ) == -1
    } {
        panic!("arch_prctl(ARCH_GET_FS) failed");
    }

    cached_data.brk = unsafe { nix::libc::sbrk(0) };

    // restore() will return (as in longjmp) into this function, which means we have to save
    // registers. We will save them to memory, so this has to happen before do_cache() triggers, so
    // we call do_cache() from inline assembly.
    unsafe {
        asm!(
            "push rbp",
            "push rbx",
            "mov [{saved_rsp}], rsp",
            "lea {saved_rsp}, [rip+2f]",
            "mov [{saved_rip}], {saved_rsp}",
            "call multiprocessing_do_cache",
            "2:",
            "pop rbx",
            "pop rbp",
            saved_rsp = in(reg) &mut cached_data.rsp,
            saved_rip = in(reg) &mut cached_data.rip,
            in("rdi") cached_data.maps.as_ptr(),
            in("rsi") cached_data.maps.len(),
            lateout("r12") _,
            lateout("r13") _,
            lateout("r14") _,
            lateout("r15") _,
            clobber_abi("C"),
        );
    }

    // This no-op shows that CACHED_DATA could have changed and we aren't going to modify it below
    drop(cached_data);
    let cached_data = unsafe { &*CACHED_DATA.get() };

    // Restore thread-local storage immediately, because malloc depends on it
    if unsafe {
        nix::libc::syscall(
            nix::libc::SYS_arch_prctl,
            0x1002, // ARCH_SET_FS
            cached_data.tls_base,
        ) == -1
    } {
        panic!("arch_prctl(ARCH_SET_FS) failed");
    }

    // Restore program break
    if unsafe { nix::libc::brk(cached_data.brk as *mut c_void) } == -1 {
        panic!("brk() failed");
    }

    let transparent_data = unsafe { &*cached_data.transparent_data.unwrap().get() };

    if !transparent_data.stack_bottom.is_null() {
        // After restore()
        unsafe {
            mman::munmap(
                transparent_data.stack_bottom as *mut c_void,
                transparent_data.stack_size,
            )
            .expect("Failed to deallocate ephemeral stack");
        }

        imp::start_subprocess(transparent_data.entry_rx_fd);
    }
}

// This subroutine must use a separate stackframe, so that it does not mingle with the cached stack
// portion
#[no_mangle]
unsafe fn multiprocessing_do_cache(maps_data: *const VMMap, maps_len: usize) {
    let maps = std::slice::from_raw_parts(maps_data, maps_len);
    for map in maps {
        if map.cache_base.is_null() {
            continue;
        }

        // TODO: instead of calling copy_nonoverlapping, we could only copy non-zero words. This
        // ensures that zero pages don't get allocated
        let from = map.range_start as *const u64;
        let to = map.cache_base as *mut u64;
        let length = map.range_end as usize - map.range_start as usize;
        std::ptr::copy_nonoverlapping(from, to, length / 8);
    }
}

#[derive(Debug)]
enum RestoreAction {
    Map(
        *mut u8,
        usize,
        mman::ProtFlags,
        mman::MapFlags,
        RawFd,
        off_t,
    ),
    Unmap(*mut u8, usize),
    Copy(*const u8, *mut u8, usize),
}

pub(crate) unsafe fn restore(entry_rx_fd: RawFd) -> ! {
    probe();

    // Two stages: firstly, figure out our actions. Secondly, carry them out on a dedicated stack
    // without accessing heap and other data that we'll roll back.

    // map and unmap memory so that regions are the same as at the moment of caching
    let cached_data = &*CACHED_DATA.get();
    let mut current_maps = get_current_maps(false);

    // Allocate the stack after current_maps to avoid clashing or whatever

    // Worst case: everything has to be remapped, plus a page for control flow stack
    let stack_size =
        std::mem::size_of::<RestoreAction>() * (cached_data.maps.len() + current_maps.len()) + 4096;
    let stack_size = (stack_size + 4095) & !4095;

    let stack_bottom = mman::mmap(
        std::ptr::null_mut(),
        stack_size,
        mman::ProtFlags::PROT_READ | mman::ProtFlags::PROT_WRITE,
        mman::MapFlags::MAP_PRIVATE | mman::MapFlags::MAP_ANONYMOUS | mman::MapFlags::MAP_STACK,
        -1,
        0,
    )
    .expect("Failed to mmap a range") as *mut u8;

    let stack_top = stack_bottom.add(stack_size);

    // Make sure this stack will be in-place after restoration
    for map in cached_data.maps.iter() {
        if map.range_start.max(stack_bottom) < map.range_end.min(stack_top) {
            panic!("The allocated stack intersects a mapping");
        }
    }

    let mut stack_head = stack_top as *mut RestoreAction;

    let mut push_action = |action: RestoreAction| {
        stack_head = stack_head.sub(1);
        std::ptr::copy_nonoverlapping(&action as *const RestoreAction, stack_head, 1);
    };

    // Firstly, unmap every region that shouldn't be present in the restored image
    let mut i = 0;
    let mut j = 0;

    let mut unmap_in_range = |start, end| {
        while i < current_maps.len() && current_maps[i].range_end < start {
            i += 1;
        }
        while j < current_maps.len() && current_maps[j].range_end < end {
            j += 1;
        }

        for k in i..=j {
            if k < current_maps.len() {
                let unmap_start = start.max(current_maps[k].range_start);
                let unmap_end = end.min(current_maps[k].range_end);
                if unmap_start < unmap_end {
                    push_action(RestoreAction::Unmap(
                        unmap_start as *mut u8,
                        unmap_end as usize - unmap_start as usize,
                    ));
                }
            }
        }
    };

    let mut prev_end: *const u8 = std::ptr::null();
    for (start, end) in cached_data
        .maps
        .iter()
        .map(|map| (map.range_start, map.range_end))
        .merge(cached_data.cache_mappings.iter().cloned())
        .merge([(stack_bottom as *const u8, stack_top as *const u8)])
    {
        unmap_in_range(prev_end, start);
        prev_end = end;
    }
    // Is there anything we have to unmap at the tail end?
    let last_end = current_maps.last().unwrap().range_end;
    if prev_end < last_end {
        unmap_in_range(prev_end, last_end);
    }

    // Secondly, map the regions that have changed
    let mut i = 0;

    for map in cached_data.maps.iter() {
        // Do we have a map just like this one?
        while i < current_maps.len() && current_maps[i].range_start < map.range_start {
            i += 1;
        }

        let mut should_map = true;
        if i < current_maps.len() {
            // For comparison
            current_maps[i].fd = map.fd;
            current_maps[i].cache_base = map.cache_base;
            should_map = current_maps[i] != *map;
        }

        if should_map {
            push_action(RestoreAction::Map(
                map.range_start as *mut u8,
                map.range_end as usize - map.range_start as usize,
                map.prot,
                mman::MapFlags::MAP_PRIVATE
                    | mman::MapFlags::MAP_FIXED
                    | (if map.fd == -1 {
                        mman::MapFlags::MAP_ANONYMOUS
                    } else {
                        mman::MapFlags::empty()
                    }),
                map.fd,
                map.offset,
            ));
        }

        if !map.cache_base.is_null() {
            push_action(RestoreAction::Copy(
                map.cache_base,
                map.range_start as *mut u8,
                map.range_end as usize - map.range_start as usize,
            ));
        }
    }

    let transparent_data = &mut *cached_data.transparent_data.unwrap().get();
    transparent_data.stack_bottom = stack_bottom;
    transparent_data.stack_size = stack_size;
    transparent_data.entry_rx_fd = entry_rx_fd;

    // Switch to the new stack
    asm!(
        "push rbp",
        "mov rbp, rsp",
        "mov rsp, {stack}",
        "and rsp, ~0xf",
        "call multiprocessing_restore_trampoline",
        "mov rsp, rbp",
        "pop rbp",
        stack = in(reg) stack_head,
        in("rdi") stack_top,
        in("rsi") stack_head,
        in("rdx") cached_data.rsp,
        in("rcx") cached_data.rip,
        clobber_abi("C"),
        options(noreturn),
    );
}

#[no_mangle]
unsafe fn multiprocessing_restore_trampoline(
    mut action_top: *const RestoreAction,
    action_head: *const RestoreAction,
    saved_rsp: *const u8,
    saved_rip: *const u8,
) {
    while action_top != action_head {
        action_top = action_top.sub(1);
        match *action_top {
            RestoreAction::Map(addr, length, prot, flags, fd, offset) => {
                mman::mmap(addr as *mut c_void, length, prot, flags, fd, offset)
                    .expect("Failed to call mmap during restoration");
            }
            RestoreAction::Unmap(addr, length) => {
                mman::munmap(addr as *mut c_void, length)
                    .expect("Failed to call munmap during restoration");
            }
            RestoreAction::Copy(from, to, size) => {
                std::ptr::copy_nonoverlapping(from, to, size);
            }
        }
    }

    asm!(
        "mov rsp, {saved_rsp}",
        "jmp rax",
        saved_rsp = in(reg) saved_rsp,
        in("rax") saved_rip,
        options(noreturn),
    );
}

fn probe() {
    // We have to be careful not to remap anything while reading /proc/self/maps, and this can
    // happen if glibc decides to allocate a new arena, or the stack grows. We attempt to prevent
    // the former by reserving enough memory and immediately releasing it, and the latter by probing
    // the stack.
    let layout = std::alloc::Layout::from_size_align(32 * 1024, 4096).unwrap();
    let allocated = std::alloc::System
        .allocate(layout)
        .expect("Failed to allocate memory");
    unsafe {
        std::alloc::System.deallocate(allocated.as_non_null_ptr(), layout);
    }

    // FIXME: This crashes valgrind:
    // unsafe {
    //     asm!(
    //         "or dword ptr [rsp - 4096], 0;",
    //         "or dword ptr [rsp - 2 * 4096], 0;",
    //         "or dword ptr [rsp - 3 * 4096], 0;",
    //         "or dword ptr [rsp - 4 * 4096], 0;",
    //         "or dword ptr [rsp - 5 * 4096], 0;",
    //         "or dword ptr [rsp - 6 * 4096], 0;",
    //         "or dword ptr [rsp - 7 * 4096], 0;",
    //         "or dword ptr [rsp - 8 * 4096], 0;",
    //         out("rax") _,
    //         options(nomem, nostack),
    //     );
    // }
}

fn get_current_maps(open_fds: bool) -> Vec<VMMap> {
    let mut maps = Vec::new();

    let mut fd_by_target = HashMap::new();

    for line in std::io::BufReader::new(
        std::fs::File::open("/proc/self/maps").expect("Failed to open /proc/self/maps"),
    )
    .lines()
    {
        let line = line.expect("Failed to read /proc/self/maps");
        let mut it = line.splitn(6, ' ');

        let range = it.next().expect("Invalid maps format: range is missing");
        let dash = range
            .find('-')
            .expect("Invalid maps format: range does not contain a dash");
        let range_start = usize::from_str_radix(&range[..dash], 16)
            .expect("Invalid maps format: range start is invalid");
        let range_end = usize::from_str_radix(&range[dash + 1..], 16)
            .expect("Invalid maps format: range end is invalid");

        let mode = it
            .next()
            .expect("Invalid maps format: mode is missing")
            .as_bytes();
        let offset = off_t::from_str_radix(
            it.next().expect("Invalid maps format: offset is missing"),
            16,
        )
        .expect("Invalid maps format: offset is invalid");

        let dev = it.next().expect("Invalid maps format: dev is missing");
        let dev_major =
            u8::from_str_radix(&dev[..2], 16).expect("Invalid maps format: dev is invalid");
        let dev_minor =
            u8::from_str_radix(&dev[3..], 16).expect("Invalid maps format: dev is invalid");

        let inode: u64 = it
            .next()
            .expect("Invalid maps format: inode is missing")
            .parse()
            .expect("Invalid maps format: inode is invalid");

        let file = it
            .next()
            .expect("Invalid maps format: file is missing")
            .trim_start();

        let mut prot = mman::ProtFlags::empty();
        if mode[0] == b'r' {
            prot |= mman::ProtFlags::PROT_READ;
        }
        if mode[1] == b'w' {
            prot |= mman::ProtFlags::PROT_WRITE;
        }
        if mode[2] == b'x' {
            prot |= mman::ProtFlags::PROT_EXEC;
        }
        if prot.is_empty() {
            prot = mman::ProtFlags::PROT_NONE;
        }

        let mut fd = -1;
        if open_fds && inode != 0 {
            // Following a symlink in map_files requires CAP_SYS_ADMIN. Reading it does not. Oh
            // well.
            // No, this is not the same as {range}, because range may contain leading zeroes
            let path = format!("/proc/self/map_files/{range_start:x}-{range_end:x}");
            let target = std::fs::read_link(&path).expect(&format!("Failed to readlink {path}"));
            fd = *fd_by_target.entry(target.clone()).or_insert_with(|| {
                nix::fcntl::open::<std::path::Path>(
                    target.as_ref(),
                    nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_CLOEXEC,
                    nix::sys::stat::Mode::empty(),
                )
                .expect(&format!("Failed to open the target of {path}"))
            });
        }

        maps.push(VMMap {
            range_start: range_start as *const u8,
            range_end: range_end as *const u8,
            prot,
            shared: mode[3] == b's',
            offset,
            dev_major,
            dev_minor,
            inode,
            file: file.to_string(),
            fd,
            cache_base: std::ptr::null_mut(),
        });
    }

    maps
}

pub(crate) fn is_retained_fd(fd: RawFd) -> bool {
    let cached_data = unsafe { &*CACHED_DATA.get() };
    cached_data.retained_fds.contains(&fd)
}
