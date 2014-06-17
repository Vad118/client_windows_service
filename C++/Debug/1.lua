function summator(param,input,cust) 
	send(cust,input+param)
	sleep(1);
end
function umnog(cust,param,input)  
	if param==nil then
		become("umnog",cust,input)
	else
		send(cust,param*input)
	end
	sleep(1);
end
--Первоначальное создание и инициализация
function createAndInitActors()
	print_act=create("final_print")
	umnog_act=create("umnog",print_act,nil)
	summator1_act=create("summator",2)
	summator2_act=create("summator",3)
	send(summator1_act,10,umnog_act);
	send(summator2_act,11,umnog_act);
end
