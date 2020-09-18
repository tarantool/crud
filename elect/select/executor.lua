local select_filters = require('elect.select.filters')

local executor = {}

function executor.execute(plan)
    local scanner = plan.scanner

    if scanner.limit == 0 then
        return {}
    end

    local filter = select_filters.gen_code(plan.filter_conditions)
    local filer_func = select_filters.compile(filter)

    local tuples = {}
    local tuples_count = 0

    local space = box.space[scanner.space_name]
    local index_obj = space.index[scanner.index_id]

    for _, tuple in index_obj:cont_pairs(scanner.value, scanner.after_tuple, {iterator = scanner.iter}) do
        local matched, early_exit = filer_func(tuple)

        if matched then
            table.insert(tuples, tuple)
            tuples_count = tuples_count + 1

            if scanner.limit ~= nil and tuples_count >= scanner.limit then
                break
            end
        elseif early_exit then
            break
        end
    end

    return tuples
end

return executor
