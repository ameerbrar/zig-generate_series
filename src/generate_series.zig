//
// This template implements an eponymous-only virtual table with a rowid and
// two columns named "a" and "b".  The table as 10 rows with fixed integer
// values. Usage example:
//     .load ./Series
//     select * from generate_series where start = 1;
//
//

const std = @import("std");
const assert = std.debug.assert;
const c = @cImport(@cInclude("sqlite3ext.h"));

// sqlite3_api has a meaningful value once 
// this library is loaded by sqlite3 and
// sqlite3_series_init is called.
var sqlite3_api: *c.sqlite3_api_routines = undefined;

// Copied from raw_c_allocator.
// Asserts allocations are within `@alignOf(std.c.max_align_t)` and directly calls
// `malloc`/`free`. Does not attempt to utilize `malloc_usable_size`.
// This allocator is safe to use as the backing allocator with
// `ArenaAllocator` for example and is more optimal in such a case
// than `c_allocator`.
const Allocator = std.mem.Allocator;
var global_allocator = &allocator_state;
var allocator_state = Allocator{
    .allocFn = alloc,
    .resizeFn = resize,
};

fn alloc(
    self: *Allocator,
    len: usize,
    ptr_align: u29,
    len_align: u29,
    ret_addr: usize,
) Allocator.Error![]u8 {
    _ = self;
    _ = len_align;
    _ = ret_addr;
    assert(ptr_align <= @alignOf(std.c.max_align_t));
    const ptr = @ptrCast([*]u8, sqlite3_api.*.malloc64.?(len) orelse return error.OutOfMemory);
    return ptr[0..len];
}

fn resize(
    self: *Allocator,
    buf: []u8,
    old_align: u29,
    new_len: usize,
    len_align: u29,
    ret_addr: usize,
) Allocator.Error!usize {
    _ = self;
    _ = old_align;
    _ = ret_addr;
    if (new_len == 0) {
        sqlite3_api.*.free.?(buf.ptr);
        return 0;
    }
    if (new_len <= buf.len) {
        return std.mem.alignAllocLen(buf.len, new_len, len_align);
    }
    return error.OutOfMemory;
}

//
// Cursor is a subclass of sqlite3_vtab_cursor which will
// serve as the underlying representation of a cursor that scans
// over rows of the result
//
const Cursor = struct {
    base: c.sqlite3_vtab_cursor, // Base class
    // Insert new fields here.
    min_value: c.sqlite3_int64,
    step:      c.sqlite3_int64,
    max_value: c.sqlite3_int64,
    value:     c.sqlite3_int64,
    rowid:     c.sqlite3_int64,
    is_desc:   bool,
};

//
// VTab is a subclass of sqlite3_vtab which is
// underlying representation of the virtual table
//
const VTab = struct {
  base: c.sqlite3_vtab,  // Base class
  // Add new fields here, as necessary
};


//
// The generateSeriesConnect() method is invoked to create a new
// series virtual table.
//
// Think of this routine as the constructor for VTab objects.
//
// All this routine needs to do is:
//
//    (1) Allocate the VTab object and initialize all fields.
//
//    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
//        result set of queries against the virtual table will look like.
//
pub fn generateSeriesConnect(
    db: ?*c.sqlite3, 
    pAux: ?*c_void, 
    argc: c_int, 
    argv: [*c]const [*c]const u8, 
    ppVTab: [*c][*c]c.sqlite3_vtab, 
    pzErr: [*c][*c]u8
) callconv(.C) c_int {
    _ = pAux;
    _ = argc;
    _ = argv;
    _ = pzErr;
    var rc: c_int = c.SQLITE_OK;
    rc = sqlite3_api.*.declare_vtab.?(db, 
        "CREATE TABLE x(value, start hidden, stop hidden, step hidden)");
    if (rc == c.SQLITE_OK) {
        const pVTab = global_allocator.create(VTab) catch return c.SQLITE_NOMEM;
        ppVTab.* = &pVTab.*.base;
        _ = sqlite3_api.*.vtab_config.?(db, c.SQLITE_VTAB_INNOCUOUS);
    }
    return rc;
}

// For convenience, define symbolic names for the index to each column.
const series_value = 0;
const series_start = 1;
const series_stop  = 2;
const series_step  = 3;
//
// This method is the destructor for VTab objects.
//
pub fn generateSeriesDisconnect(pVTab: [*c]c.sqlite3_vtab) callconv(.C) c_int {
    global_allocator.destroy(@fieldParentPtr(VTab, "base", pVTab));
    return c.SQLITE_OK;
}

//
// SQLite will invoke this method one or more times while planning a query
// that uses the virtual table.  This routine needs to create
// a query plan for each invocation and compute an estimated cost for that
// plan.
//
pub fn generateSeriesBestIndex(pVTab: [*c]c.sqlite3_vtab, pIdxInfo: [*c]c.sqlite3_index_info) callconv(.C) c_int {
    _ = pVTab;
    // This implementation assumes that the start, stop, and step columns
    // are the last three columns in the virtual table.
    assert(series_stop == series_start + 1);
    assert(series_step == series_start + 2);  

    var i: usize = 0;
    var start_seen = false;
    var unusableMask: u3 = 0;  // Mask of unusable constraints ('100', '101', '111'...)
    var idxNum: u5 = 0;        // The query plan bitmask '000' (step, stop, start)
    var aIdx: [3]?usize = .{null, null, null}; // start, stop, step: (index in aConstraint)
    while (i < pIdxInfo.*.nConstraint) : (i += 1) {
        const constraint = pIdxInfo.*.aConstraint[i];

        if (constraint.iColumn < series_start) {
            continue;
        }
        const iCol = constraint.iColumn - series_start; // 0 for start, 1 for stop, 2 for step
        assert(iCol >= 0 and iCol <= 2);
        const iMask: u3 = @intCast(u3, 1) << @intCast(u2, iCol); // '001' for start, '010' for stop, '100' for step (in bits)
        if (iCol == 0) {
            start_seen = true;
        }
        if (constraint.usable == @boolToInt(false)) {
            unusableMask |= iMask;
            continue;
        } else if (constraint.op == c.SQLITE_INDEX_CONSTRAINT_EQ) {
            idxNum |= iMask;
            aIdx[@intCast(u2, iCol)] = i;
        }
    }
    // argvIndex = 1 is the first item.
    var nArg: c_int = 1; // Number of arguments that seriesFilter() expects
    for (aIdx) |optional_index| {
        if (optional_index) |index| {
            defer nArg += 1;
            pIdxInfo.*.aConstraintUsage[index].argvIndex = nArg;
            pIdxInfo.*.aConstraintUsage[index].omit = @boolToInt(true);
        }
    }
    if((unusableMask & ~idxNum)!= 0) {
        // I don't understand exactly what this is checking.
        // When is this condition true?
        // The start, stop, and step columns are inputs.  Therefore if there
        // are unusable constraints on any of start, stop, or step then
        // this plan is unusable
        // Isn't this the same as unusableMask != 0? Why do we need to & with ~idxNum?
        // '111' & ~'000' == '111' & '111' == '111' != '000' (All un-usable)
        // '001' & ~'110' == '001' & '001' == '001' != '000' (Start un-usable)
        // '010' & ~'101' == '010' & '010' == '010' != '000' (Stop un-usable)
        // '000' & ~'000' == '000' & '111' == '000' == '000' (no constraints) **
        // The last case shows why this is used instead of just checking unusableMask != 0.
        // This accounts for the case where no constraints are supplied by the caller but that is still valid.
        return c.SQLITE_CONSTRAINT;
    }

    if ((idxNum & 3) == 3) {
        // 3 is '011' so start and stop are supplied
        // This also accounts for the case where idxNum === '111'
        pIdxInfo.*.estimatedCost = @intToFloat(f64, @as(c_int, 10));
        pIdxInfo.*.estimatedRows = 100;
        // Handle the order by clause
        if (pIdxInfo.*.nOrderBy == 1) {
            if (pIdxInfo.*.aOrderBy[0].desc == @boolToInt(true)) {
                idxNum |= 8;  // '01000'
            } else {
                idxNum |= 16; // '10000'
            }
            // Tells sqlite3 that we will handle ordering the output
            pIdxInfo.*.orderByConsumed = @boolToInt(true);
        }
    } else {
        // Start or stop is missing,
        // So make this case very expensive so that the query
        // planner will work hard to avoid it.
        pIdxInfo.*.estimatedRows = 2147483647;
    }
    pIdxInfo.*.idxNum = idxNum; 
    return c.SQLITE_OK;
}  

//
// This method is called to "rewind" the cursor object back
// to the first row of output.  This method is always called at least
// once prior to any call to generateSeriesColumn() or generateSeriesRowid() or 
// generateSeriesEof().
//
pub fn generateSeriesFilter(
    pCursor: [*c]c.sqlite3_vtab_cursor, 
    idxNum: c_int, 
    idxStr: [*c]const u8, 
    argc: c_int, 
    argv: [*c]?*c.sqlite3_value
) callconv(.C) c_int {
    _ = idxStr;
    _ = argc;

    var pCur = @fieldParentPtr(Cursor, "base", pCursor);
    // idxNum is b'xyabc' where x,y,a,b,c in {0, 1}
    var i: usize =  0; // index into argv
    var is_desc = (idxNum & 8) != 0;
    if ((idxNum & 1) != 0) { // Start is present
        pCur.*.min_value = sqlite3_api.*.value_int64.?(argv[i]);
        i += 1;
    } else {
        pCur.*.min_value = 0;
    }
    if ((idxNum & 2) != 0) { // stop is present
        pCur.*.max_value = sqlite3_api.*.value_int64.?(argv[i]);
        i += 1;
    } else {
        pCur.*.max_value = 0xffffffff;
    }
    if ((idxNum & 4) != 0) { // step is present
        pCur.*.step = sqlite3_api.*.value_int64.?(argv[i]);
        defer i += 1;
        if (pCur.*.step == 0) { // step = 0 doesn't make sense
            pCur.*.step = 1;
        } else if (pCur.*.step < 0) {
            // (i.e) select * from generate_series(1, 10, -1);
            pCur.*.step = pCur.*.step * -1;
            // Change the order to descending 
            // only if the user hasn't already said they want ascending - (i.e) say idxNum is '10111'
            // in which case this turns into - select * from generate_series(1, 10, 1);
            if ((idxNum & 16) == 0) {
                is_desc = true; // idxNum |= 8;
            }
        }
    } else {
        pCur.*.step = 1;
    }

    if (is_desc) { // is descending
        pCur.*.is_desc = true;
        pCur.*.value = pCur.*.max_value;
    } else { // default ascending
        pCur.*.is_desc = false;
        pCur.*.value = pCur.*.min_value;
    }
    pCur.*.rowid = 1;

    return c.SQLITE_OK;
}

//
// Constructor for a new cursor object.
//
pub fn generateSeriesOpen(pVTab: [*c]c.sqlite3_vtab, ppCursor: [*c][*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
    _ = pVTab;
    const pCur = global_allocator.create(Cursor) catch return c.SQLITE_NOMEM;
    ppCursor.* = &pCur.*.base;
    return c.SQLITE_OK;
}

//
// Destructor for a cursor.
//
pub fn generateSeriesClose(pCursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
    global_allocator.destroy(@fieldParentPtr(Cursor, "base", pCursor));
    return c.SQLITE_OK;
}

//
// Return values of columns for the row at which the cursor
// is currently pointing.
//
pub fn generateSeriesColumn(pCursor: [*c]c.sqlite3_vtab_cursor, cxt: ?*c.sqlite3_context, n: c_int) callconv(.C) c_int {
    const pCur = @fieldParentPtr(Cursor, "base", pCursor);
    const x = switch (n) {
        series_start => pCur.min_value,
        series_step => pCur.step,
        series_stop => pCur.max_value,
        else => pCur.value,
    };
    sqlite3_api.*.result_int64.?(cxt, x);
    return c.SQLITE_OK;
}

//
// Return the rowid for the current row.
//
pub fn generateSeriesRowid(pCursor: [*c]c.sqlite3_vtab_cursor, pRowid: [*c]c.sqlite3_int64) callconv(.C) c_int {
    var pCur = @fieldParentPtr(Cursor, "base", pCursor);
    pRowid.* = pCur.rowid;
    return c.SQLITE_OK;
}

//
// Advance a cursor to its next row of output.
//
pub fn generateSeriesNext(pCursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
    var pCur = @fieldParentPtr(Cursor, "base", pCursor);
    pCur.*.rowid += 1;
    if (pCur.*.is_desc) {
        pCur.*.value -= pCur.*.step;
    } else {
        pCur.*.value += pCur.*.step;
    }
    return c.SQLITE_OK;
}

//
// Return TRUE if the cursor has been moved off of the last
// row of output.
//
pub fn generateSeriesEof(pCursor: [*c]c.sqlite3_vtab_cursor) callconv(.C) c_int {
    var pCur = @fieldParentPtr(Cursor, "base", pCursor);
    if (pCur.is_desc) {
        return @boolToInt(pCur.value < pCur.min_value);
    } else {
        return @boolToInt(pCur.value > pCur.max_value);
    }
}

// "eponymous virtual tables": exist automatically in the "main" schema of every database connection in which their module is registered
// To make your VT eponymous, make the xCreate method NULL.

//
// This following structure defines all the methods for the 
// virtual table.
//
const generateSeriesModule = c.sqlite3_module {
    .iVersion = 0,
    .xCreate = null,
    .xConnect = generateSeriesConnect,
    .xBestIndex = generateSeriesBestIndex,
    .xDisconnect = generateSeriesDisconnect,
    .xDestroy = null,
    .xOpen = generateSeriesOpen,
    .xClose = generateSeriesClose,
    .xFilter = generateSeriesFilter,
    .xNext = generateSeriesNext,
    .xEof = generateSeriesEof,
    .xColumn = generateSeriesColumn,
    .xRowid = generateSeriesRowid,
    .xUpdate = null,
    .xBegin = null,
    .xSync = null,
    .xCommit = null,
    .xRollback = null,
    .xFindFunction = null,
    .xRename = null,
    .xSavepoint = null,
    .xRelease = null,
    .xRollbackTo = null,
    .xShadowName = null,
};

pub export fn sqlite3_series_init(db: ?*c.sqlite3, pzErrMsg: [*c][*c]u8, pApi: [*c]c.sqlite3_api_routines) c_int {
    _ = pzErrMsg;
    var rc: c_int = c.SQLITE_OK;
    sqlite3_api = pApi.?;
    rc = sqlite3_api.*.create_module.?(db, "generate_series", &generateSeriesModule, null);
    return rc;
}