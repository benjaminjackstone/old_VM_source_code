filename = "fractal.ppm"
xcenter = -0.743643135
ycenter = 0.131825963
mag = 91152.35
xsize = 800
ysize = 600
iters = 1000

function mandel(maxiters, x, y)
    local a = x
    local b = y
    for i = 1, maxiters do
        local a2 = a * a
        local b2 = b * b
        if a2 + b2 >= 4.0 then
            return i
        end
        local ab = a * b
        b = ab + ab + y
        a = a2 - b2 + x
    end
    return 0
end

function calcPixel(col, row, sizeX, sizeY, centerX, centerY, magnification)
    local minsize = sizeX
    if sizeY < sizeX then
        minsize = sizeY
    end
    local x = centerX + (col - sizeX/2) / (magnification * (minsize-1))
    local y = centerY - (row - sizeY/2) / (magnification * (minsize-1))
    local i = cmandel(iters, x, y)
    if i == 0 then
        return 0, 0, 0
    else
        return 0, i%256, 0
    end
end

function generateImage()
    local file = io.open(filename, "w")
    file:write("P3\n"..xsize.." "..ysize.."\n255\n")
    for row = 0, ysize-1 do
        for col = 0, xsize-1 do
            r, g, b = calcPixel(col, row, xsize, ysize, xcenter, ycenter, mag)
            file:write(r.." "..g.." "..b.." ")
        end
        file:write("\n")
    end
    file:close()
end

-- generateImage()
