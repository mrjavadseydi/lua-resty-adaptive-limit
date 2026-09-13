-- Exponentially weighted moving average update.
--
--   new = current + alpha * (sample - current)
--
-- alpha in (0, 1]: 1 replaces the current value entirely, small values
-- adapt slowly (the long-term RTT baseline uses ~0.05 for exactly that
-- reason: it must not learn a short overload episode as the new normal).
--
-- Seeding: a nil current adopts the sample verbatim (first observation).

return function(current, sample, alpha)
    if current == nil then
        return sample
    end
    return current + alpha * (sample - current)
end
