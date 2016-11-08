-- create text--

function makeTable(tokens, n)
  local lookup_table = {}
  local shingle_table = {}
  local table_length = #tokens + 1

  for i = 0, #tokens do
    local shingle = {}
    for j = 1, n do
      shingle[j] = tokens[(i + j -1 ) % table_length ]
    end

    -- Values are keys 1 to n-1 for table concat. 0 is ignored
    -- Keys 1 to n-1 as key for lookup_table table
    local suffix = tokens[(i + n - 1) % table_length]
	local new_key = {}
	for k=1,#shingle - 1 do
		new_key[k] = shingle[k]
	end

    	local key = table.concat(new_key, " ")
	lookup_table[i] = key

    -- Set empty table first time this key is used
    if lookup_table[key] == nil then
      lookup_table[key] = {}
    end

    --Appending to an array
    lookup_table[key][#lookup_table[key] + 1] = suffix

    -- Add last value to shingle for shingle_table
    shingle[n] = suffix
    shingle_table[i+1] = shingle
  end

  return lookup_table, shingle_table
end

function babble(start, table, size)
	local ngram = {}
	ngram = start
	print('\n')
	--Generate ngrams
	for i=0,size do
		newgram = {}
		for j=1,#ngram - 1 do
			newgram[j] = ngram[j+1]
		end

	local prefix = ""
	--Concat
	for index = 1, #newgram do
		if index == 1 then 
			prefix = prefix..newgram[index] 
		else
			prefix = prefix.." "..newgram[index]
		end
	end

	candidates = table[prefix]
	suffix = candidates[math.random(#candidates)]
	newgram[#newgram + 1] = suffix
	ngram = newgram
	io.write(suffix, " ")
	end
end


function main(file, size, n)
	math.randomseed( os.time() )
	local tokens = {}
	local offset = 0;
	local index = 0
	local raw_text = creadfile(file)
	local token, offset = cmakeTokens(raw_text, 0)
	
  	while offset < string.len(raw_text) do
    		tokens[index] = token
    		index = index + 1
    		token, offset = cmakeTokens(raw_text, offset)
 	end
	
	tokens[index] = token
 	lookup_table, shingle_table = makeTable(tokens, n)
	babble(shingle_table[math.random(index)],lookup_table, size)
	
end
