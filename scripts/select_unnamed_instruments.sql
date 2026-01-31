-- Select instruments that aren't associated with an entity
SELECT ipm.provider,
       ipm.provider_code AS symbol,
       ipm.instrument_id
FROM instrument_provider_map AS ipm
LEFT JOIN instrument_entity AS ie
  ON ie.instrument_id = ipm.instrument_id
WHERE ie.instrument_id IS NULL
ORDER BY ipm.provider, ipm.provider_code;