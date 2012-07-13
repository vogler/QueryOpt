close all;
clear all;

m = 100;
n = 10;

N = n*m;
Y = zeros(1,N);
Y_w = zeros(1,N);
Y_b = zeros(1,N);


for k = 1:1000
    if (k<=N-n) 
        %p = nchoosek(N-n,k)/nchoosek(N,k); %not exact
        %Yao
        X = 0:k-1;
        p = prod((N-n-X)./(N-X));
        Y(k) = (1-p)*m;
        %Waters
        Y_w(k) = m*(1-(1-k/N)^n);
    else
        Y(k) = m;
        Y_w(k) = m;
    end
    
    %Bernstein
    if (k<m/2)
        Y_b(k)=k;
    elseif (k<2*m)
        Y_b(k)=(k+m)/3;
    else
        Y_b(k) = m;
    end
end

title('combined plot');
ylabel('accessed pages');
xlabel('tuples');
plot(Y, 'Color', 'Red');
hold all;
plot(Y_w, 'Color', 'Blue');
hold all;
plot(Y_b, 'Color', 'Green');

figure;
title('Yao');
ylabel('accessed pages');
xlabel('tuples');
plot(Y, 'Color', 'Red');

figure;
title('Waters');
ylabel('accessed pages');
xlabel('tuples');
plot(Y_w, 'Color', 'Blue');

figure;
title('Bernstein');
ylabel('accessed pages');
xlabel('tuples');
plot(Y_b, 'Color', 'Green');

